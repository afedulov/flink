/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.TaskThreadInfoSampleResponse;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ThreadInfo;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

// TODO: iron out subsamples/samples desctiption in comments
/** A coordinator for triggering and collecting thread info samples of running tasks. */
public class ThreadInfoSampleCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadInfoSampleCoordinator.class);

    private static final int NUM_GHOST_SAMPLE_IDS = 10;

    private final Object lock = new Object();

    /** Executor used to run the futures. */
    private final Executor executor;

    /** Time out after the expected sampling duration. */
    private final long sampleTimeout;

    /** In progress samples (guarded by lock). */
    private final Map<Integer, PendingThreadInfoSample> pendingSamples = new HashMap<>();

    /** A list of recent sample IDs to identify late messages vs. invalid ones. */
    private final ArrayDeque<Integer> recentPendingSamples = new ArrayDeque<>(NUM_GHOST_SAMPLE_IDS);

    /** Sample ID counter (guarded by lock). */
    private int sampleIdCounter;

    /** Flag indicating whether the coordinator is still running (guarded by lock). */
    private boolean isShutDown;

    /**
     * Creates a new coordinator for the job.
     *
     * @param executor to use to execute the futures
     * @param sampleTimeout Time out after the expected sampling duration. This is added to the
     *     expected duration of a sample, which is determined by the number of samples and the delay
     *     between each sample.
     */
    public ThreadInfoSampleCoordinator(Executor executor, long sampleTimeout) {
        checkArgument(sampleTimeout >= 0L);
        this.executor = Preconditions.checkNotNull(executor);
        this.sampleTimeout = sampleTimeout;
    }

    /**
     * Triggers collection of thread info samples for given execution vertices. Each sample consists
     * of {@code numSubSamples} subsamples, collected with {@code delayBetweenSamples} milliseconds
     * delay between them.
     *
     * @param executionsWithGateways Execution vertices together with TaskExecutors running them.
     * @param numSubSamples Number of thread info subsamples to collect.
     * @param delayBetweenSamples Delay between consecutive subsamples (ms).
     * @param maxStackTraceDepth Maximum depth of the stack traces collected within thread info
     *     samples.
     * @return A future of the completed thread info sample
     */
    public CompletableFuture<ThreadInfoSample> triggerThreadInfoSample(
            List<Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>>
                    executionsWithGateways,
            int numSubSamples,
            Time delayBetweenSamples,
            int maxStackTraceDepth) {

        checkNotNull(executionsWithGateways, "Tasks to sample");
        checkArgument(executionsWithGateways.size() > 0, "No tasks to sample");
        checkArgument(numSubSamples >= 1, "No number of samples");
        checkArgument(maxStackTraceDepth >= 0, "Negative maximum stack trace depth");

        // Execution IDs of running tasks
        List<ExecutionAttemptID> triggerIds = new ArrayList<>();

        // TODO: skip this check for archived execution vertices?
        // Check that all tasks are RUNNING before triggering anything. The
        // triggering can still fail.
        for (Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>
                executionsWithGateway : executionsWithGateways) {
            AccessExecution execution = executionsWithGateway.f0.getCurrentExecutionAttempt();
            if (execution != null && execution.getState() == ExecutionState.RUNNING) {
                triggerIds.add(execution.getAttemptId());
            } else {
                return FutureUtils.completedExceptionally(
                        new IllegalStateException(
                                "Task "
                                        + executionsWithGateway.f0.getTaskNameWithSubtaskIndex()
                                        + " is not running."));
            }
        }

        synchronized (lock) {
            if (isShutDown) {
                return FutureUtils.completedExceptionally(new IllegalStateException("Shut down"));
            }

            final int sampleId = sampleIdCounter++;

            LOG.debug("Triggering thread info sample {}", sampleId);

            final PendingThreadInfoSample pending =
                    new PendingThreadInfoSample(sampleId, triggerIds);

            // Discard the sample if it takes too long. We don't send cancel
            // messages to the task managers, but only wait for the responses
            // and then ignore them.
            long expectedDuration = numSubSamples * delayBetweenSamples.toMilliseconds();
            Time timeout = Time.milliseconds(expectedDuration + sampleTimeout);

            // Add the pending sample before scheduling the discard task to
            // prevent races with removing it again.
            pendingSamples.put(sampleId, pending);

            // Trigger all samples
            for (Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>
                    executionWithGateway : executionsWithGateways) {

                CompletableFuture<TaskExecutorGateway> executorGatewayFuture =
                        executionWithGateway.f1;
                ExecutionAttemptID attemptId =
                        executionWithGateway.f0.getCurrentExecutionAttempt().getAttemptId();

                CompletableFuture<TaskThreadInfoSampleResponse> threadInfo =
                        executorGatewayFuture.thenCompose(
                                executorGateway ->
                                        executorGateway.requestThreadInfoSamples(
                                                attemptId,
                                                sampleId,
                                                numSubSamples,
                                                delayBetweenSamples,
                                                maxStackTraceDepth,
                                                timeout));

                threadInfo.handleAsync(
                        (TaskThreadInfoSampleResponse threadInfoSamplesResponse,
                                Throwable throwable) -> {
                            if (threadInfoSamplesResponse != null) {
                                collectStackTraces(
                                        threadInfoSamplesResponse.getSampleId(),
                                        threadInfoSamplesResponse.getExecutionAttemptID(),
                                        threadInfoSamplesResponse.getSamples());
                            } else {
                                cancelStackTraceSample(sampleId, throwable);
                            }
                            return null;
                        },
                        executor);
            }

            return pending.getThreadInfoSampleFuture();
        }
    }

    /**
     * Cancels a pending sample.
     *
     * @param sampleId ID of the sample to cancel.
     * @param cause Cause of the cancelling (can be <code>null</code>).
     */
    public void cancelStackTraceSample(int sampleId, Throwable cause) {
        synchronized (lock) {
            if (isShutDown) {
                return;
            }

            PendingThreadInfoSample sample = pendingSamples.remove(sampleId);
            if (sample != null) {
                if (cause != null) {
                    LOG.info("Cancelling sample " + sampleId, cause);
                } else {
                    LOG.info("Cancelling sample {}", sampleId);
                }

                sample.discard(cause);
                rememberRecentSampleId(sampleId);
            }
        }
    }

    /**
     * Shuts down the coordinator.
     *
     * <p>After shut down, no further operations are executed.
     */
    public void shutDown() {
        synchronized (lock) {
            if (!isShutDown) {
                LOG.info("Shutting down stack trace sample coordinator.");

                for (PendingThreadInfoSample pending : pendingSamples.values()) {
                    pending.discard(new RuntimeException("Shut down"));
                }

                pendingSamples.clear();

                isShutDown = true;
            }
        }
    }

    /**
     * Collects thread infos of a task.
     *
     * @param sampleId ID of the sample.
     * @param executionId ID of the sampled task.
     * @param threadInfoSubSamples Thread info subsamples of the task.
     * @throws IllegalStateException If unknown sample ID and not recently finished or cancelled
     *     sample.
     */
    public void collectStackTraces(
            int sampleId, ExecutionAttemptID executionId, List<ThreadInfo> threadInfoSubSamples) {

        synchronized (lock) {
            if (isShutDown) {
                return;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Collecting thread info sample {} of task {}", sampleId, executionId);
            }

            PendingThreadInfoSample pending = pendingSamples.get(sampleId);

            if (pending != null) {
                pending.collectThredInfoSubsamples(executionId, threadInfoSubSamples);

                // Publish the sample
                if (pending.isComplete()) {
                    pendingSamples.remove(sampleId);
                    rememberRecentSampleId(sampleId);

                    pending.completePromiseAndDiscard();
                }
            } else if (recentPendingSamples.contains(sampleId)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Received late thread info sample {} of task {}",
                            sampleId,
                            executionId);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Unknown sample ID " + sampleId);
                }
            }
        }
    }

    private void rememberRecentSampleId(int sampleId) {
        if (recentPendingSamples.size() >= NUM_GHOST_SAMPLE_IDS) {
            recentPendingSamples.removeFirst();
        }
        recentPendingSamples.addLast(sampleId);
    }

    int getNumberOfPendingSamples() {
        synchronized (lock) {
            return pendingSamples.size();
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A pending thread info sample, which collects multiple subsamples and owns a {@link
     * ThreadInfoSample} promise.
     *
     * <p>Access pending sample in lock scope.
     */
    private static class PendingThreadInfoSample {

        private final int sampleId;
        private final long startTime;
        private final Set<ExecutionAttemptID> pendingTasks;
        private final Map<ExecutionAttemptID, List<ThreadInfo>> threadInfoSubSamplesByTask;
        private final CompletableFuture<ThreadInfoSample> resultFuture;

        private boolean isDiscarded;

        PendingThreadInfoSample(int sampleId, List<ExecutionAttemptID> tasksToCollect) {

            this.sampleId = sampleId;
            this.startTime = System.currentTimeMillis();
            this.pendingTasks = new HashSet<>(tasksToCollect);
            this.threadInfoSubSamplesByTask =
                    Maps.newHashMapWithExpectedSize(tasksToCollect.size());
            this.resultFuture = new CompletableFuture<>();
        }

        int getSampleId() {
            return sampleId;
        }

        long getStartTime() {
            return startTime;
        }

        boolean isDiscarded() {
            return isDiscarded;
        }

        boolean isComplete() {
            if (isDiscarded) {
                throw new IllegalStateException("Discarded");
            }

            return pendingTasks.isEmpty();
        }

        void discard(Throwable cause) {
            if (!isDiscarded) {
                pendingTasks.clear();
                threadInfoSubSamplesByTask.clear();

                resultFuture.completeExceptionally(new RuntimeException("Discarded", cause));

                isDiscarded = true;
            }
        }

        void collectThredInfoSubsamples(
                ExecutionAttemptID executionId, List<ThreadInfo> threadInfoSubSamples) {
            if (isDiscarded) {
                throw new IllegalStateException("Discarded");
            }

            if (pendingTasks.remove(executionId)) {
                threadInfoSubSamplesByTask.put(
                        executionId, Collections.unmodifiableList(threadInfoSubSamples));
            } else if (isComplete()) {
                throw new IllegalStateException("Completed");
            } else {
                throw new IllegalArgumentException("Unknown task " + executionId);
            }
        }

        void completePromiseAndDiscard() {
            if (isComplete()) {
                isDiscarded = true;

                long endTime = System.currentTimeMillis();

                ThreadInfoSample threadInfoSample =
                        new ThreadInfoSample(
                                sampleId, startTime, endTime, threadInfoSubSamplesByTask);

                resultFuture.complete(threadInfoSample);
            } else {
                throw new IllegalStateException("Not completed yet");
            }
        }

        CompletableFuture<ThreadInfoSample> getThreadInfoSampleFuture() {
            return resultFuture;
        }
    }
}

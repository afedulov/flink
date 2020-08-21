/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.util.JvmUtils;

import javax.annotation.Nonnegative;

import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Samples the stack traces of tasks. */
class ThreadInfoSampleService {

    private final ScheduledExecutor scheduledExecutor;

    ThreadInfoSampleService(final ScheduledExecutor scheduledExecutor) {
        this.scheduledExecutor =
                requireNonNull(scheduledExecutor, "scheduledExecutor must not be null");
    }

    /**
     * //TODO: REWRITE
     *
     * <p>Returns a future that completes with a given number of stack trace samples of a task
     * thread.
     *
     * @param task The task to be sampled from.
     * @param numSubSamples The number of samples.
     * @param delayBetweenSamples The time to wait between taking samples.
     * @param maxStackTraceDepth The maximum depth of the returned stack traces. Negative means
     *     unlimited.
     * @return A future containing the stack trace samples.
     */
    public CompletableFuture<List<ThreadInfo>> requestThreadInfoSamples(
            final Task task,
            @Nonnegative final int numSubSamples,
            final Time delayBetweenSamples,
            final int maxStackTraceDepth) {

        checkNotNull(task, "task must not be null");
        checkArgument(numSubSamples > 0, "numSamples must be positive");
        checkNotNull(delayBetweenSamples, "delayBetweenSamples must not be null");

        return requestThreadInfoSamples(
                task,
                numSubSamples,
                delayBetweenSamples,
                maxStackTraceDepth,
                new ArrayList<>(numSubSamples),
                new CompletableFuture<>());
    }

    private CompletableFuture<List<ThreadInfo>> requestThreadInfoSamples(
            final Task task,
            final int numSubSamples,
            final Time delayBetweenSamples,
            final int maxStackTraceDepth,
            final List<ThreadInfo> currentTraces,
            final CompletableFuture<List<ThreadInfo>> resultFuture) {

        final long threadId = task.getExecutingThread().getId();
        final Optional<ThreadInfo> stackTrace =
                JvmUtils.createThreadInfo(threadId, maxStackTraceDepth);

        if (stackTrace.isPresent()) {
            currentTraces.add(stackTrace.get());
        } else if (!currentTraces.isEmpty()) {
            resultFuture.complete(currentTraces);
            return resultFuture;
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Cannot sample task %s. " + "The task is not running.",
                            task.getExecutionId()));
        }

        if (numSubSamples > 1) {
            scheduledExecutor.schedule(
                    () ->
                            requestThreadInfoSamples(
                                    task,
                                    numSubSamples - 1,
                                    delayBetweenSamples,
                                    maxStackTraceDepth,
                                    currentTraces,
                                    resultFuture),
                    delayBetweenSamples.getSize(),
                    delayBetweenSamples.getUnit());
        } else {
            resultFuture.complete(currentTraces);
        }
        return resultFuture;
    }
}

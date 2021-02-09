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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertexTest;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.util.JvmUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link ThreadInfoOperatorTracker}. */
public class ThreadInfoOperatorTrackerTest extends TestLogger {

    private static final int REQUEST_ID = 0;
    private static final ExecutionJobVertex EXECUTION_JOB_VERTEX = createExecutionJobVertex();
    private static final ExecutionVertex[] TASK_VERTICES = EXECUTION_JOB_VERTEX.getTaskVertices();

    private static ThreadInfoSample threadInfoSample;
    private static OperatorThreadInfoStats threadInfoStatsDefaultSample;

    private static final int CLEAN_UP_INTERVAL = 60000;
    private static final int STATS_REFRESH_INTERVAL = 60000;
    private static final long TIME_GAP = 60000;
    private static final long SMALL_TIME_GAP = 1;
    private static final long REQUEST_TIMEOUT = 10000;

    private static final int PARALLELISM = 4;
    private static final int NUMBER_OF_SAMPLES = 1;
    private static final int MAX_STACK_TRACE_DEPTH = 100;
    private static final Time DELAY_BETWEEN_SAMPLES = Time.milliseconds(50);

    @Rule public Timeout caseTimeout = new Timeout(10, TimeUnit.SECONDS);

    @BeforeClass
    public static void setUp() {
        // Time gap determines endTime of stats, which controls if the "refresh" is triggered:
        // now >= stats.getEndTime() + statsRefreshInterval
        // Using a small gap to be able to test cache updates without much delay.
        threadInfoSample =
                JvmUtils.createThreadInfoSample(
                                Thread.currentThread().getId(), MAX_STACK_TRACE_DEPTH)
                        .get();
        threadInfoStatsDefaultSample =
                createThreadInfoStats(
                        REQUEST_ID, SMALL_TIME_GAP, Collections.singletonList(threadInfoSample));
    }

    /** Tests successful thread info stats request. */
    @Test
    public void testGetThreadInfoStats() throws Exception {
        doInitialRequestAndVerifyResult(createThreadInfoTracker());
    }

    /** Tests that cached result is reused within refresh interval. */
    @Test
    public void testCachedStatsNotUpdatedWithinRefreshInterval() throws Exception {
        final int requestId2 = 1;

        final OperatorThreadInfoStats threadInfoStats2 =
                createThreadInfoStats(requestId2, TIME_GAP, null);

        final ThreadInfoOperatorTracker<OperatorThreadInfoStats> tracker =
                createThreadInfoTracker(
                        CLEAN_UP_INTERVAL,
                        STATS_REFRESH_INTERVAL,
                        threadInfoStatsDefaultSample,
                        threadInfoStats2);
        // stores threadInfoStatsDefaultSample in cache
        doInitialRequestAndVerifyResult(tracker);
        Optional<OperatorThreadInfoStats> result = tracker.getOperatorStats(EXECUTION_JOB_VERTEX);
        // cached result is returned instead of threadInfoStats2
        assertEquals(threadInfoStatsDefaultSample, result.get());
    }

    /** Tests that cached result is NOT reused after refresh interval. */
    @Test
    public void testCachedStatsUpdatedAfterRefreshInterval() throws Exception {
        final int threadInfoStatsRefreshInterval2 = 10;
        final long waitingTime = threadInfoStatsRefreshInterval2 + 10;

        final int requestId2 = 1;
        final OperatorThreadInfoStats threadInfoStats2 =
                createThreadInfoStats(
                        requestId2, TIME_GAP, Collections.singletonList(threadInfoSample));

        final ThreadInfoOperatorTracker<OperatorThreadInfoStats> tracker =
                createThreadInfoTracker(
                        CLEAN_UP_INTERVAL,
                        threadInfoStatsRefreshInterval2,
                        threadInfoStatsDefaultSample,
                        threadInfoStats2);
        doInitialRequestAndVerifyResult(tracker);

        // ensure that the previous request "expires"
        Thread.sleep(waitingTime);

        Optional<OperatorThreadInfoStats> result = tracker.getOperatorStats(EXECUTION_JOB_VERTEX);

        assertExpectedEqualsReceived(threadInfoStats2, result);

        assertNotSame(result.get(), threadInfoStatsDefaultSample);
    }

    /** Tests that cached results are removed within the cleanup interval. */
    @Test
    public void testCachedStatsCleanedAfterCleanupInterval() throws Exception {
        final int cleanUpInterval2 = 10;
        final long waitingTime = cleanUpInterval2 + 10;

        final ThreadInfoOperatorTracker<OperatorThreadInfoStats> tracker =
                createThreadInfoTracker(
                        cleanUpInterval2, STATS_REFRESH_INTERVAL, threadInfoStatsDefaultSample);
        doInitialRequestAndVerifyResult(tracker);

        // wait until we are ready to cleanup
        Thread.sleep(waitingTime);

        // cleanup the cached thread info stats
        tracker.cleanUpOperatorStatsCache();
        assertFalse(tracker.getOperatorStats(EXECUTION_JOB_VERTEX).isPresent());
    }

    /** Tests that cached results are NOT removed within the cleanup interval. */
    @Test
    public void testCachedStatsNotCleanedWithinCleanupInterval() throws Exception {
        final ThreadInfoOperatorTracker<OperatorThreadInfoStats> tracker =
                createThreadInfoTracker();

        doInitialRequestAndVerifyResult(tracker);

        tracker.cleanUpOperatorStatsCache();
        // the thread info stats with the same requestId should still be there
        assertExpectedEqualsReceived(
                threadInfoStatsDefaultSample, tracker.getOperatorStats(EXECUTION_JOB_VERTEX));
    }

    /** Tests that cached results are not served after the shutdown. */
    @Test
    public void testShutDown() throws Exception {
        final ThreadInfoOperatorTracker<OperatorThreadInfoStats> tracker =
                createThreadInfoTracker();
        doInitialRequestAndVerifyResult(tracker);

        // shutdown directly
        tracker.shutDown();

        // verify that the previous cached result is invalid and trigger another request
        assertFalse(tracker.getOperatorStats(EXECUTION_JOB_VERTEX).isPresent());
        // verify no response after shutdown
        assertFalse(tracker.getOperatorStats(EXECUTION_JOB_VERTEX).isPresent());
    }

    private void doInitialRequestAndVerifyResult(
            ThreadInfoOperatorTracker<OperatorThreadInfoStats> tracker)
            throws InterruptedException, ExecutionException {
        assertFalse(tracker.getOperatorStats(EXECUTION_JOB_VERTEX).isPresent());
        // block until the async call completes and the first result is available
        tracker.getResultAvailableFuture().get();
        assertExpectedEqualsReceived(
                threadInfoStatsDefaultSample, tracker.getOperatorStats(EXECUTION_JOB_VERTEX));
    }

    private void assertExpectedEqualsReceived(
            OperatorThreadInfoStats expected, Optional<OperatorThreadInfoStats> receivedOptional) {
        assertTrue(receivedOptional.isPresent());
        OperatorThreadInfoStats received = receivedOptional.get();

        assertEquals(expected.getRequestId(), received.getRequestId());
        assertEquals(expected.getEndTime(), received.getEndTime());

        assertEquals(TASK_VERTICES.length, received.getNumberOfSubtasks());

        for (List<ThreadInfoSample> samples : received.getSamplesBySubtask().values()) {
            assertThat(samples.isEmpty(), is(false));
        }
    }

    private ThreadInfoOperatorTracker<OperatorThreadInfoStats> createThreadInfoTracker() {
        return createThreadInfoTracker(
                CLEAN_UP_INTERVAL, STATS_REFRESH_INTERVAL, threadInfoStatsDefaultSample);
    }

    private ThreadInfoOperatorTracker<OperatorThreadInfoStats> createThreadInfoTracker(
            int cleanUpInterval,
            int threadInfoStatsRefreshInterval,
            OperatorThreadInfoStats... stats) {

        final ThreadInfoRequestCoordinator coordinator =
                new TestingThreadInfoRequestCoordinator(Runnable::run, REQUEST_TIMEOUT, stats);

        final TestingGatewayRetriever resourceManagerRetriever = new TestingGatewayRetriever();

        ExecutorService executor = Executors.newSingleThreadExecutor();

        return ThreadInfoOperatorTracker.newBuilder(
                        resourceManagerRetriever, Function.identity(), executor)
                .setCoordinator(coordinator)
                .setCleanUpInterval(cleanUpInterval)
                .setNumSamples(NUMBER_OF_SAMPLES)
                .setStatsRefreshInterval(threadInfoStatsRefreshInterval)
                .setDelayBetweenSamples(DELAY_BETWEEN_SAMPLES)
                .setMaxThreadInfoDepth(MAX_STACK_TRACE_DEPTH)
                .build();
    }

    private static OperatorThreadInfoStats createThreadInfoStats(
            int requestId, long timeGap, List<ThreadInfoSample> threadInfoSamples) {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeGap;

        final Map<ExecutionAttemptID, List<ThreadInfoSample>> threadInfoRatiosByTask =
                new HashMap<>();

        for (ExecutionVertex vertex : TASK_VERTICES) {
            threadInfoRatiosByTask.put(
                    vertex.getCurrentExecutionAttempt().getAttemptId(), threadInfoSamples);
        }

        return new OperatorThreadInfoStats(requestId, startTime, endTime, threadInfoRatiosByTask);
    }

    private static ExecutionJobVertex createExecutionJobVertex() {
        try {
            return ExecutionJobVertexTest.createExecutionJobVertex(PARALLELISM, PARALLELISM);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create ExecutionJobVertex.");
        }
    }

    private static class TestingGatewayRetriever
            implements GatewayRetriever<ResourceManagerGateway> {

        @Override
        public CompletableFuture<ResourceManagerGateway> getFuture() {
            return CompletableFuture.completedFuture(createMockResourceManagerGateway());
        }
    }

    private static TestingResourceManagerGateway createMockResourceManagerGateway() {

        // ignored in TestingThreadInfoRequestCoordinator
        Function<ResourceID, CompletableFuture<TaskExecutorGateway>> function =
                (resourceID) -> CompletableFuture.completedFuture(null);

        TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        testingResourceManagerGateway.setRequestTaskExecutorGatewayFunction(function);
        return testingResourceManagerGateway;
    }

    /**
     * A {@link ThreadInfoRequestCoordinator} which returns the pre-generated thread info stats
     * directly.
     */
    private static class TestingThreadInfoRequestCoordinator extends ThreadInfoRequestCoordinator {

        private final OperatorThreadInfoStats[] operatorThreadInfoStats;
        private int counter = 0;

        TestingThreadInfoRequestCoordinator(
                Executor executor,
                long requestTimeout,
                OperatorThreadInfoStats... operatorThreadInfoStats) {
            super(executor, requestTimeout);
            this.operatorThreadInfoStats = operatorThreadInfoStats;
        }

        @Override
        public CompletableFuture<OperatorThreadInfoStats> triggerThreadInfoRequest(
                List<Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>>
                        ignored1,
                int ignored2,
                Time ignored3,
                int ignored4) {
            return CompletableFuture.completedFuture(
                    operatorThreadInfoStats[(counter++) % operatorThreadInfoStats.length]);
        }
    }
}

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

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * To be delivered.
 *
 * @param <T> Type of statistics to track.
 */
public class StackTraceOperatorTracker<T extends Stats> implements OperatorStatsTracker<T> {

	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

	/**
	 * Create a new {@link Builder}.
	 * @param createStatsFn Function that converts stack trace sample to a statistic.
	 * @param <T> Type of statistics to track.
	 * @return Builder.
	 */
	public static <T extends Stats> Builder<T> newBuilder(
		GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
		BiFunction<ExecutionJobVertex, StackTraceSample, T> createStatsFn) {
		return new Builder<>(resourceManagerGatewayRetriever, createStatsFn);
	}

	/**
	 * To be delivered.
	 *
	 * @param <T> Type of statistics to track.
	 */
	public static class Builder<T extends Stats> {

		private final BiFunction<ExecutionJobVertex, StackTraceSample, T> createStatsFn;
		private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

		private StackTraceSampleCoordinator coordinator;
		private int cleanUpInterval;
		private int numSamples;
		private int statsRefreshInterval;
		private Time delayBetweenSamples;
		private int maxStackTraceDepth = 0;

		private Builder(
			GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			BiFunction<ExecutionJobVertex, StackTraceSample, T> createStatsFn) {
			this.createStatsFn = createStatsFn;
			this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
		}

		public Builder<T> setCoordinator(StackTraceSampleCoordinator coordinator) {
			this.coordinator = coordinator;
			return this;
		}

		public Builder<T> setCleanUpInterval(int cleanUpInterval) {
			this.cleanUpInterval = cleanUpInterval;
			return this;
		}

		public Builder<T> setNumSamples(int numSamples) {
			this.numSamples = numSamples;
			return this;
		}

		public Builder<T> setStatsRefreshInterval(int statsRefreshInterval) {
			this.statsRefreshInterval = statsRefreshInterval;
			return this;
		}

		public Builder<T> setDelayBetweenSamples(Time delayBetweenSamples) {
			this.delayBetweenSamples = delayBetweenSamples;
			return this;
		}

		public Builder<T> setMaxStackTraceDepth(int maxStackTraceDepth) {
			this.maxStackTraceDepth = maxStackTraceDepth;
			return this;
		}

		public StackTraceOperatorTracker<T> build() {
			return new StackTraceOperatorTracker<>(
				coordinator,
				resourceManagerGatewayRetriever,
				createStatsFn,
				cleanUpInterval,
				numSamples,
				statsRefreshInterval,
				delayBetweenSamples,
				maxStackTraceDepth);
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(StackTraceOperatorTracker.class);

	/** Lock guarding trigger operations. */
	private final Object lock = new Object();

	/* Stack trace sample coordinator. */
	private final StackTraceSampleCoordinator coordinator;

	private final BiFunction<ExecutionJobVertex, StackTraceSample, T> createStatsFn;

	/**
	 * Completed stats. Important: Job vertex IDs need to be scoped by job ID,
	 * because they are potentially constant across runs messing up the cached
	 * data.
	 */
	private final Cache<ExecutionJobVertex, T> operatorStatsCache;

	/** Pending in progress stats. Important: Job vertex IDs need to be scoped
	 * by job ID, because they are potentially constant across runs messing up
	 * the cached data.*/
	private final Set<AccessExecutionJobVertex> pendingStats = new HashSet<>();

	private final int numSamples;

	private final int statsRefreshInterval;

	private final Time delayBetweenSamples;

	private final int maxStackTraceDepth;

	/** Flag indicating whether the stats tracker has been shut down. */
	private boolean shutDown;

	/**
	 * Creates a back pressure statistics tracker.
	 *
	 * @param cleanUpInterval     Clean up interval for completed stats.
	 * @param numSamples          Number of stack trace samples when determining back pressure.
	 * @param delayBetweenSamples Delay between samples when determining back pressure.
	 */
	private StackTraceOperatorTracker(
		StackTraceSampleCoordinator coordinator,
		GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
		BiFunction<ExecutionJobVertex, StackTraceSample, T> createStatsFn,
		int cleanUpInterval,
		int numSamples,
		int statsRefreshInterval,
		Time delayBetweenSamples,
		int maxStackTraceDepth) {

		this.coordinator = checkNotNull(coordinator, "Stack trace sample coordinator");
		this.resourceManagerGatewayRetriever = checkNotNull(resourceManagerGatewayRetriever, "Gateway retriever");
		this.createStatsFn = checkNotNull(createStatsFn, "Create stats function");

		checkArgument(cleanUpInterval >= 0, "Clean up interval");

		checkArgument(numSamples >= 1, "Number of samples");
		this.numSamples = numSamples;

		checkArgument(
			statsRefreshInterval >= 0,
			"Stats refresh interval must be greater than or equal to 0");
		this.statsRefreshInterval = statsRefreshInterval;

		this.delayBetweenSamples = checkNotNull(delayBetweenSamples, "Delay between samples");

		checkArgument(
			maxStackTraceDepth >= 0,
			"Max stack trace depth must be greater than or equal to 0");
		this.maxStackTraceDepth = maxStackTraceDepth;

		this.operatorStatsCache = CacheBuilder.newBuilder()
				.concurrencyLevel(1)
				.expireAfterAccess(cleanUpInterval, TimeUnit.MILLISECONDS)
				.build();
	}

	@Override
	public Optional<T> getOperatorStats(ExecutionJobVertex vertex) {
		synchronized (lock) {
			final T stats = operatorStatsCache.getIfPresent(vertex);
			if (stats == null || statsRefreshInterval <= System.currentTimeMillis() - stats.getEndTimestamp()) {
				triggerStackTraceSampleInternal(vertex);
			}
			return Optional.ofNullable(stats);
		}
	}


	public Optional<T> getOperatorStats(AccessExecutionJobVertex vertex) {
		synchronized (lock) {
			final T stats = operatorStatsCache.getIfPresent(vertex);
			if (stats == null || statsRefreshInterval <= System.currentTimeMillis() - stats.getEndTimestamp()) {
				triggerStackTraceSampleInternal(vertex);
			}
			return Optional.ofNullable(stats);
		}
	}

// TODO: discuss how to handle exceptions from here
//	public ResourceManagerGateway getResourceManagerGateway() throws RestHandlerException {
	public ResourceManagerGateway getResourceManagerGateway() {
		return resourceManagerGatewayRetriever.getNow().get();
//			.getNow()
//			.orElseThrow(() -> new RestHandlerException(
//				"Cannot connect to ResourceManager right now. Please try to refresh.",
//				HttpResponseStatus.NOT_FOUND));
	}


	/**
	 * Triggers a stack trace sample for a operator to gather the back pressure
	 * statistics. If there is a sample in progress for the operator, the call
	 * is ignored.
	 *
	 * @param vertex Operator to get the stats for.
	 */
	private void triggerStackTraceSampleInternal(final ExecutionJobVertex vertex) {
		assert(Thread.holdsLock(lock));

		if (!shutDown &&
			!pendingStats.contains(vertex) &&
			!vertex.getGraph().getState().isGloballyTerminalState()) {

			Executor executor = vertex.getGraph().getFutureExecutor();

			// Only trigger if still active job
			if (executor != null) {
				pendingStats.add(vertex);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Triggering stack trace sample for tasks: " + Arrays.toString(vertex.getTaskVertices()));
				}

				CompletableFuture<StackTraceSample> sample = coordinator.triggerStackTraceSample(
					vertex.getTaskVertices(),
					numSamples,
					delayBetweenSamples,
					maxStackTraceDepth);

				sample.handleAsync(new StackTraceSampleCompletionCallback(vertex), executor);
			}
		}
	}


	/**
	 * Triggers a stack trace sample for a operator to gather the back pressure
	 * statistics. If there is a sample in progress for the operator, the call
	 * is ignored.
	 *
	 * @param vertex Operator to get the stats for.
	 */
	private void triggerStackTraceSampleInternal(final AccessExecutionJobVertex vertex) {
		assert(Thread.holdsLock(lock));

		if (!shutDown &&
			!pendingStats.contains(vertex)
			//TODO: check what to do with the archived graph
//			&&
//			!vertex.getGraph().getState().isGloballyTerminalState()) {
		){
			//TODO: not available - check which executor to use?
//			Executor executor = vertex.getGraph().getFutureExecutor();
			ExecutorService executor = Executors.newSingleThreadExecutor();


			// Only trigger if still active job
			if (executor != null) {
				pendingStats.add(vertex);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Triggering stack trace sample for tasks: " + Arrays.toString(vertex.getTaskVertices()));
				}

				AccessExecutionVertex[] execuctionVertices = vertex.getTaskVertices();
				List<Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>> executionsWithGateways = new ArrayList<>();
				for (AccessExecutionVertex executionVertex : execuctionVertices) {
					TaskManagerLocation tmLocation = executionVertex.getCurrentAssignedResourceLocation();
					CompletableFuture<TaskExecutorGateway> taskExecutorGatewayFuture =
						getResourceManagerGateway().getTaskExecutorGateway(tmLocation.getResourceID());

					taskExecutorGatewayFuture.handle((gateway, throwable) -> {
						return FutureUtils.completedExceptionally(new IllegalStateException("Task " + executionsWithGateway.f0
							.getTaskNameWithSubtaskIndex() + " is not running."));
						});

					executionsWithGateways.add(new Tuple2<>(executionVertex, taskExecutorGatewayFuture));
				}

				CompletableFuture<StackTraceSample> sample = coordinator.triggerStackTraceSample(
					executionsWithGateways,
					numSamples,
					delayBetweenSamples,
					maxStackTraceDepth);

				sample.handleAsync(new StackTraceSampleCompletionCallback(vertex), executor);
			}
		}
	}


	@Override
	public void cleanUpOperatorStatsCache() {
		operatorStatsCache.cleanUp();
	}

	@Override
	public void shutDown() {
		synchronized (lock) {
			if (!shutDown) {
				operatorStatsCache.invalidateAll();
				pendingStats.clear();

				shutDown = true;
			}
		}
	}

	/**
	 * Callback on completed stack trace sample.
	 */
	class StackTraceSampleCompletionCallback implements BiFunction<StackTraceSample, Throwable, Void> {

		private final ExecutionJobVertex vertex;

		StackTraceSampleCompletionCallback(ExecutionJobVertex vertex) {
			this.vertex = vertex;
		}

		@Override
		public Void apply(StackTraceSample stackTraceSample, Throwable throwable) {
			synchronized (lock) {
				try {
					if (shutDown) {
						return null;
					}
					// Job finished, ignore.
					final JobStatus jobState = vertex.getGraph().getState();
					if (jobState.isGloballyTerminalState()) {
						LOG.debug("Ignoring sample, because job is in state " + jobState + ".");
					} else if (stackTraceSample != null) {
						operatorStatsCache.put(
							vertex, createStatsFn.apply(vertex, stackTraceSample));
					} else {
						LOG.debug("Failed to gather stack trace sample.", throwable);
					}
				} catch (Throwable t) {
					LOG.error("Error during stats completion.", t);
				} finally {
					pendingStats.remove(vertex);
				}
				return null;
			}
		}
	}
}

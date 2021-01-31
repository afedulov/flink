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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.webmonitor.stats.Stats;

import java.lang.management.ThreadInfo;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A sample describing thread details for one or more tasks. Each sample contains a number of
 * subsamples for statistical purposes.
 *
 * <p>The sampling is triggered in {@link ThreadInfoSampleCoordinator}.
 */
public class ThreadInfoSample implements Stats {

    /** ID of this sample (unique per job). */
    private final int sampleId;

    /** Timestamp, when the sample was triggered. */
    private final long startTime;

    /** Timestamp, when all subsamples were collected */
    private final long endTime;

    /** Map of thread info subsamples by execution ID. */
    private final Map<ExecutionAttemptID, List<ThreadInfo>> subSamplesByTask;

    /**
     * Creates a thread details sample.
     *
     * @param sampleId ID of the sample.
     * @param startTime Timestamp, when the sample was triggered.
     * @param endTime Timestamp, when all thread info subsamples were collected.
     * @param subSamplesByTask Map of thread info subsamples by task (execution ID).
     */
    public ThreadInfoSample(
            int sampleId,
            long startTime,
            long endTime,
            Map<ExecutionAttemptID, List<ThreadInfo>> subSamplesByTask) {

        checkArgument(sampleId >= 0, "Negative sample ID");
        checkArgument(startTime >= 0, "Negative start time");
        checkArgument(endTime >= startTime, "End time before start time");

        this.sampleId = sampleId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.subSamplesByTask = Collections.unmodifiableMap(subSamplesByTask);
    }

    /**
     * Returns the ID of the sample.
     *
     * @return ID of the sample
     */
    public int getSampleId() {
        return sampleId;
    }

    /**
     * Returns the timestamp, when the sample was triggered.
     *
     * @return Timestamp, when the sample was triggered
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the timestamp, when all subsamples where collected.
     *
     * @return Timestamp, when all subsamples where collected
     */
    @Override
    public long getEndTime() {
        return endTime;
    }

    /**
     * Returns the a map of thread info subsamples by task (execution ID).
     *
     * @return Map of thread info subsamples by task (execution ID)
     */
    public Map<ExecutionAttemptID, List<ThreadInfo>> getSubSamples() {
        return subSamplesByTask;
    }

    @Override
    public String toString() {
        return "ThreadInfoSample{"
                + "sampleId="
                + sampleId
                + ", startTime="
                + startTime
                + ", endTime="
                + endTime
                + '}';
    }
}

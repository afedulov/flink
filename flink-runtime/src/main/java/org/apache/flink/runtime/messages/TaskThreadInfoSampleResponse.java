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

package org.apache.flink.runtime.messages;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.lang.management.ThreadInfo;
import java.util.List;

/** Response to the request to collect thread details samples. */
public class TaskThreadInfoSampleResponse implements Serializable {

    private static final long serialVersionUID = -4786454630050578031L;

    /** ID of this sample (unique per job). */
    private final int sampleId;

    /** ID of the execution attempt (Task) being sampled. */
    private final ExecutionAttemptID executionAttemptID;

    /** Thread info samples. */
    private final List<ThreadInfo> samples;

    /**
     * Creates a response to the request to collect thread details samples.
     *
     * @param sampleId ID of the sample.
     * @param executionAttemptID ID of the execution attempt (Task) being sampled.
     * @param samples Thread info samples..
     */
    public TaskThreadInfoSampleResponse(
            int sampleId, ExecutionAttemptID executionAttemptID, List<ThreadInfo> samples) {
        this.sampleId = sampleId;
        this.executionAttemptID = Preconditions.checkNotNull(executionAttemptID);
        this.samples = Preconditions.checkNotNull(samples);
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
     * Returns the ID of the execution attempt (Task) being sampled.
     *
     * @return ID of the execution attempt (Task) being sampled
     */
    public ExecutionAttemptID getExecutionAttemptID() {
        return executionAttemptID;
    }

    /**
     * Returns the a map of thread info subsamples by task (execution ID).
     *
     * @return Map of thread info subsamples by task (execution ID)
     */
    public List<ThreadInfo> getSamples() {
        return samples;
    }
}

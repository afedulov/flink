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

import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;

import java.util.Optional;

/** {@link OperatorStatsTracker} implementation which always returns no statistics. */
public class VoidOperatorStatsTracker<T extends Stats> implements OperatorStatsTracker<T> {

    private static final VoidOperatorStatsTracker<?> INSTANCE = new VoidOperatorStatsTracker<>();

    /**
     * Get {@link VoidOperatorStatsTracker} instance.
     *
     * @param <T> Type of statistics to track.
     * @return No-op instance.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Stats> VoidOperatorStatsTracker<T> getInstance() {
        return (VoidOperatorStatsTracker) INSTANCE;
    }

    private VoidOperatorStatsTracker() {}

    @Override
    public Optional<T> getOperatorStats(AccessExecutionJobVertex vertex) {
        return Optional.empty();
    }

    @Override
    public void cleanUpOperatorStatsCache() {
        // nothing to do
    }

    @Override
    public void shutDown() {
        // nothing to do
    }
}

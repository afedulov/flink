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

package org.apache.flink.api.connector.source.lib.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * An implementation of {@link RateLimiter} that completes defined number of futures in-between the
 * external notification events. The first cycle completes immediately, without waiting for the
 * external notifications.
 */
public class GatedRateLimiter implements RateLimiter {

    private final int capacityPerCycle;
    private int capacityLeft;

    /**
     * Instantiates a new GatedRateLimiter.
     *
     * @param capacityPerCycle The number of completed futures per cycle.
     */
    public GatedRateLimiter(int capacityPerCycle) {
        this.capacityPerCycle = capacityPerCycle;
        this.capacityLeft = capacityPerCycle + 1;
    }

    CompletableFuture<Void> gatingFuture;

    @Override
    public CompletionStage<Void> acquire() {
        if (capacityLeft-- > 0) {
            return CompletableFuture.completedFuture(null);
        } else {
            if (gatingFuture == null) {
                gatingFuture = new CompletableFuture<>();
            }
            return gatingFuture;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        capacityLeft = capacityPerCycle - 1;
        if (gatingFuture != null) { // for bounded data can be called twice without acquire()
            gatingFuture.complete(null);
        }
        gatingFuture = null;
    }
}

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

package org.apache.flink.api.common.io.ratelimiting;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.RateLimiter;

/** An implementation of {@link RateLimiter} based on Guava's RateLimiter. */
public class GuavaRateLimiter implements org.apache.flink.api.common.io.ratelimiting.RateLimiter {

    private final RateLimiter rateLimiter;

    public GuavaRateLimiter(long maxPerSecond, int numParallelExecutors) {
        final float maxPerSecondPerSubtask = (float) maxPerSecond / numParallelExecutors;
        this.rateLimiter = RateLimiter.create(maxPerSecondPerSubtask);
    }

    @Override
    public int acquire() {
        return (int) rateLimiter.acquire();
    }
}

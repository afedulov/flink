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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Utility to throttle a thread to a given number of executions (records) per second. */
public final class SimpleRateLimiter implements RateLimiter {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleRateLimiter.class);

    private long maxPerBucket;
    private long nanosInBucket;

    private long endOfCurrentBucketNanos;
    private int inCurrentBucket;

    private static final int DEFAULT_BUCKETS_PER_SECOND = 10;
    private static final long NANOS_IN_ONE_SECOND = 1_000_000_000L;

    public SimpleRateLimiter(long maxPerSecond, int numParallelExecutors) {
        this(maxPerSecond, numParallelExecutors, DEFAULT_BUCKETS_PER_SECOND);
    }

    public SimpleRateLimiter(long maxPerSecond, int numParallelExecutors, int bucketsPerSecond) {
        checkArgument(maxPerSecond > 0, "maxPerSecond must be a positive number");
        checkArgument(numParallelExecutors > 0, "numParallelExecutors must be greater than 0");

        final float maxPerSecondPerSubtask = (float) maxPerSecond / numParallelExecutors;

        maxPerBucket = ((int) (maxPerSecondPerSubtask / bucketsPerSecond)) + 1;
        nanosInBucket = ((int) (NANOS_IN_ONE_SECOND / maxPerSecondPerSubtask)) * maxPerBucket;

        this.endOfCurrentBucketNanos = System.nanoTime() + nanosInBucket;
        this.inCurrentBucket = 0;
    }

    // TODO: JavaDoc, returns number of seconds idling on this call.
    public int acquire() throws InterruptedException {
        LOG.error("THROTTLE!");
        if (++inCurrentBucket != maxPerBucket) {
            return 0;
        }
        // The current bucket is "full". Wait until the next bucket.
        final long now = System.nanoTime();
        final int millisRemaining = (int) ((endOfCurrentBucketNanos - now) / 1_000_000);
        inCurrentBucket = 0;

        if (millisRemaining > 0) {
            endOfCurrentBucketNanos += nanosInBucket;
            Thread.sleep(millisRemaining);
            return millisRemaining;
        } else {
            // Throttle was not called often enough so that the bucket's capacity was not exhausted
            // "in time". We need to push the bucket's "end time" further to compensate and avoid
            // bursts in case polling behaviour catches up.
            endOfCurrentBucketNanos = now + nanosInBucket;
            return 0;
        }
    }
}

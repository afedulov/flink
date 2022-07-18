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

package org.apache.flink.api.connector.source.lib;

import org.apache.flink.api.common.io.ratelimiting.GuavaRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.RateLimiter;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.util.GeneratingIteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.RateLimitedSourceReader;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class GeneratorSourceReaderFactory<OUT>
        implements SourceReaderFactory<OUT, NumberSequenceSource.NumberSequenceSplit> {

    private final GeneratorFunction<Long, OUT> generatorFunction;
    private final long sourceRatePerSecond;

    public GeneratorSourceReaderFactory(
            GeneratorFunction<Long, OUT> generatorFunction, long sourceRatePerSecond) {
        this.generatorFunction = checkNotNull(generatorFunction);
        this.sourceRatePerSecond = sourceRatePerSecond;
    }

    @Override
    public SourceReader<OUT, NumberSequenceSource.NumberSequenceSplit> newSourceReader(
            SourceReaderContext readerContext) {
        if (sourceRatePerSecond > 0) {
            int parallelism = readerContext.currentParallelism();
            RateLimiter rateLimiter = new GuavaRateLimiter(sourceRatePerSecond, parallelism);
            return new RateLimitedSourceReader<>(
                    new GeneratingIteratorSourceReader<>(readerContext, generatorFunction),
                    rateLimiter);
        } else {
            return new GeneratingIteratorSourceReader<>(readerContext, generatorFunction);
        }
    }
}

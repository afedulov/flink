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

package org.apache.flink.connector.datagen.source;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceReaderFactory;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.GuavaRateLimiter;
import org.apache.flink.api.connector.source.lib.util.RateLimitedSourceReader;
import org.apache.flink.api.connector.source.lib.util.RateLimiter;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A factory for instantiating source readers that produce elements by applying a user-supplied
 * {@link GeneratorFunction}.
 *
 * @param <OUT> The type of the output elements.
 */
public class GeneratorSourceReaderFactory<OUT>
        implements SourceReaderFactory<OUT, NumberSequenceSource.NumberSequenceSplit> {

    private final GeneratorFunction<Long, OUT> generatorFunction;
    private final double sourceRatePerSecond;

    /**
     * Instantiates a new {@code GeneratorSourceReaderFactory}.
     *
     * @param generatorFunction The generator function.
     * @param maxSourceRatePerSecond The target source rate per second. This parameter specifies the
     *     overall source rate (across all source subtasks).
     */
    public GeneratorSourceReaderFactory(
            GeneratorFunction<Long, OUT> generatorFunction, double maxSourceRatePerSecond) {
        this.generatorFunction = checkNotNull(generatorFunction);
        this.sourceRatePerSecond = maxSourceRatePerSecond;
    }

    @Override
    public SourceReader<OUT, NumberSequenceSource.NumberSequenceSplit> createReader(
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

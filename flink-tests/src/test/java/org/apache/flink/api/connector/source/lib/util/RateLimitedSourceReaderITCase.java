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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.DataGeneratorSource;
import org.apache.flink.api.connector.source.lib.GeneratorFunction;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.SourceReaderFactory;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** An integration test for rate limiting built into the DataGeneratorSource. */
public class RateLimitedSourceReaderITCase extends TestLogger {

    private static final int PARALLELISM = 4;

    @RegisterExtension
    private static final MiniClusterExtension miniClusterExtension =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    // ------------------------------------------------------------------------

    @Test
    @DisplayName("Rate limiter is used correctly.")
    public void testRateLimitingParallelExecution() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        final int count = 10;
        final MockRateLimitingSourceReaderFactory<Long> sourceReaderFactory =
                new MockRateLimitingSourceReaderFactory<>(index -> index, 1);

        final DataGeneratorSource<Long> dataGeneratorSource =
                new DataGeneratorSource<>(sourceReaderFactory, 10, Types.LONG);

        final DataStream<Long> stream =
                env.fromSource(
                        dataGeneratorSource, WatermarkStrategy.noWatermarks(), "generator source");

        final List<Long> result = stream.executeAndCollect(10000);
        int rateLimiterCallCount = MockRateLimitingSourceReaderFactory.getRateLimitersCallCount();

        assertThat(result).containsExactlyInAnyOrderElementsOf(range(0, 9));
        assertThat(rateLimiterCallCount).isGreaterThan(count);
    }

    private List<Long> range(int startInclusive, int endInclusive) {
        return LongStream.rangeClosed(startInclusive, endInclusive)
                .boxed()
                .collect(Collectors.toList());
    }

    private static final class MockRateLimitingSourceReaderFactory<OUT>
            implements SourceReaderFactory<OUT, NumberSequenceSource.NumberSequenceSplit> {

        private final GeneratorFunction<Long, OUT> generatorFunction;
        private final double sourceRatePerSecond;

        public MockRateLimitingSourceReaderFactory(
                GeneratorFunction<Long, OUT> generatorFunction, double sourceRatePerSecond) {
            this.generatorFunction = checkNotNull(generatorFunction);
            checkArgument(sourceRatePerSecond > 0, "Source rate has to be a positive number");
            this.sourceRatePerSecond = sourceRatePerSecond;
        }

        private static final List<MockRateLimiter> rateLimiters = new ArrayList<>();

        @Override
        public SourceReader<OUT, NumberSequenceSource.NumberSequenceSplit> newSourceReader(
                SourceReaderContext readerContext) {
            MockRateLimiter rateLimiter = new MockRateLimiter();
            rateLimiters.add(rateLimiter);
            return new RateLimitedSourceReader<>(
                    new GeneratingIteratorSourceReader<>(readerContext, generatorFunction),
                    rateLimiter);
        }

        public static int getRateLimitersCallCount() {
            return rateLimiters.stream().mapToInt(MockRateLimiter::getCallCount).sum();
        }
    }

    private static final class MockRateLimiter implements RateLimiter {

        int callCount;

        @Override
        public int acquire() {
            callCount++;
            return 0;
        }

        public int getCallCount() {
            return callCount;
        }
    }
}
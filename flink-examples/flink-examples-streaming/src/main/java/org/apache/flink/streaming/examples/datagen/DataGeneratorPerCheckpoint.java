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

package org.apache.flink.streaming.examples.datagen;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SourceReaderFactory;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.GatedRateLimiter;
import org.apache.flink.api.connector.source.lib.util.RateLimitedSourceReader;
import org.apache.flink.api.connector.source.lib.util.RateLimiter;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratingIteratorSourceReader;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** TODO: remove after consensus in review. */
public class DataGeneratorPerCheckpoint {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        final String[] elements = new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
        final int size = elements.length;
        final GeneratorFunction<Long, String> generatorFunction =
                index -> elements[(int) (index % size)];
        final RateLimiter rateLimiter = new GatedRateLimiter(size);

        final SourceReaderFactory<String, NumberSequenceSource.NumberSequenceSplit> factory =
                context ->
                        new RateLimitedSourceReader<>(
                                new GeneratingIteratorSourceReader<>(context, generatorFunction),
                                rateLimiter);

        final DataGeneratorSource<String> generatorSource =
                new DataGeneratorSource<>(factory, Long.MAX_VALUE, Types.STRING);

        final DataStreamSource<String> streamSource =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");
        streamSource.print();

        env.execute("Data Generator Source Example");
    }
}

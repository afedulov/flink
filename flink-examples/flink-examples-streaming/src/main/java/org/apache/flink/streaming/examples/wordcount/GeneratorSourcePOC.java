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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.DataGeneratorSourceV4;
import org.apache.flink.api.connector.source.lib.GeneratorFunction;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class GeneratorSourcePOC {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = 2;
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelism);

        MapFunction<Long, String> generator = index -> ">>> " + index;

        /* V0 */
        /*
        DataGeneratorSourceV0<String> source =
                new DataGeneratorSourceV0<>(generator, 1000, Types.STRING);
        */

        /* V1 */
        /*
                DataGeneratorSource<String> source =
                        new DataGeneratorSource<>(generator, 1000, 2, Types.STRING);
        */

        /* V2 */
        /*

                DataGeneratorSourceV2<String> source =
                        new DataGeneratorSourceV2<>(generator, 1000, 2, Types.STRING);
        */

        /* V3 */
        /*
               DataGeneratorSourceV3<String> source =
                       new DataGeneratorSourceV3<>(generator, 1000, 2, Types.STRING);
        */

        GeneratorFunction<Long, String> generatorFunction =
                new GeneratorFunction<Long, String>() {

                    transient SourceReaderMetricGroup sourceReaderMetricGroup;

                    @Override
                    public void open(SourceReaderContext readerContext) {
                        sourceReaderMetricGroup = readerContext.metricGroup();
                    }

                    @Override
                    public String map(Long value) {
                        return "Generated: >> "
                                + value.toString()
                                + "; local metric group: "
                                + sourceReaderMetricGroup.hashCode();
                    }
                };

        GeneratorFunction<Long, String> generatorFunctionStateless = index -> ">>> " + index;

        DataGeneratorSourceV4<String> source =
                new DataGeneratorSourceV4<>(generatorFunction, 1000, 2, Types.STRING);

        DataStreamSource<String> watermarked =
                env.fromSource(
                        source,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)),
                        "watermarked");
        watermarked.print();

        env.execute("Generator Source POC");
    }
}

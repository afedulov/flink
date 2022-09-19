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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.datagen.DataGeneratorSource;
import org.apache.flink.api.connector.source.datagen.GeneratorFunction;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.utils.AttributeMap;

import java.util.Properties;

/** A simple example of generating data with {@link DataGeneratorSource}. */
public class KinesisSSLCheck {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        /*
        Properties consumerConfig = new Properties();
        consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
        consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "test");
        consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "test");
        consumerConfig.put(
                ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                ConsumerConfigConstants.InitialPosition.TRIM_HORIZON.name());
        consumerConfig.put(
                ConsumerConfigConstants.AWS_ENDPOINT, "https://localhost.localstack.cloud:4566");

        consumerConfig.put(ConsumerConfigConstants.TRUST_ALL_CERTIFICATES, "true");

        DataStream<String> kinesis =
                env.addSource(
                        new FlinkKinesisConsumer<>(
                                "test", new SimpleStringSchema(), consumerConfig));*/

        GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;
        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(generatorFunction, Long.MAX_VALUE, 1, Types.STRING);
        DataStreamSource<String> streamSource =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Data Generator");

        AttributeMap.Builder bProp = AttributeMap.builder();
        bProp.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true);

        Properties sinkProperties = new Properties();
        // Required
        sinkProperties.put(AWSConfigConstants.AWS_REGION, "us-east-1");

        // Optional, provide via alternative routes e.g. environment variables
        sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "test");
        sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "test");
        sinkProperties.put(ConsumerConfigConstants.AWS_ENDPOINT, "https://localhost:4566");
        sinkProperties.put(
                ConsumerConfigConstants.TRUST_ALL_CERTIFICATES,
                bProp.get(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES));
        //                "true");

        KinesisStreamsSink<String> kdsSink =
                KinesisStreamsSink.<String>builder()
                        .setKinesisClientProperties(sinkProperties) // Required
                        .setSerializationSchema(new SimpleStringSchema()) // Required
                        .setPartitionKeyGenerator(
                                element -> String.valueOf(element.hashCode())) // Required
                        .setStreamName("test-sink") // Required
                        .build();

        streamSource.sinkTo(kdsSink);

        streamSource.print();
        env.execute("Data Generator Source Example");
    }
}

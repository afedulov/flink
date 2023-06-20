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

package org.apache.flink.table.examples.java.connectors;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class SocketSourceExample {

    public static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()));

    public static final DataType PHYSICAL_DATA_TYPE = SCHEMA.toPhysicalRowDataType();

    public static final RowType PHYSICAL_TYPE = (RowType) PHYSICAL_DATA_TYPE.getLogicalType();

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String hostname = params.get("hostname", "localhost");
        final String port = params.get("port", "1234");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ScanContext context = ScanRuntimeProviderContext.INSTANCE;
        ChangelogCsvFormat changelogCsvFormat = new ChangelogCsvFormat("|");
        DeserializationSchema<RowData> deserializationSchema =
                changelogCsvFormat.createRuntimeDecoder(context, PHYSICAL_DATA_TYPE);

        //        DataStreamSource<String> socketSource =
        //                env.fromSource(
        //                        new SocketSource(
        //                                hostname,
        //                                Integer.parseInt(port),
        //                                (byte) 10,
        //                                new SimpleStringSchema()),
        //                        WatermarkStrategy.noWatermarks(),
        //                        "Socket Source");

        DataStreamSource<RowData> socketSource =
                env.fromSource(
                        new SocketSource(
                                hostname, Integer.parseInt(port), (byte) 10, deserializationSchema),
                        WatermarkStrategy.noWatermarks(),
                        "Socket Source");

        socketSource.print();
        env.execute();
    }
}

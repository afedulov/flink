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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.DelimitedFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** MiniCluster-based integration test for reading CSV data from {@link FileSource}. */
public class FileSourceCsvITCase extends TestLogger {

    private static final int PARALLELISM = 4;

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    // ------------------------------------------------------------------------
    //  test cases
    // ------------------------------------------------------------------------

    /** This test runs a job reading bounded input with a stream record format (text lines). */
    @Test
    public void testBoundedTextFileSource() throws Exception {
        final File testDir = TMP_FOLDER.newFolder();
        final String[] csvLines =
                new String[] {
                    "Berlin,52.5167,13.3833,Germany,DE,Berlin,primary,3644826",
                    "San Francisco,37.7562,-122.4430,United States,US,California,,3592294",
                    "Beijing,39.9050,116.3914,China,CN,Beijing,primary,19433000"
                };

        final Row[] expected =
                new Row[] {
                    Row.of(
                            "Berlin",
                            new BigDecimal("52.5167"),
                            new BigDecimal("13.3833"),
                            "Germany",
                            "DE",
                            "Berlin",
                            "primary",
                            3644826L),
                    Row.of(
                            "San Francisco",
                            new BigDecimal("37.7562"),
                            new BigDecimal("-122.4430"),
                            "United States",
                            "US",
                            "California",
                            "",
                            3592294L),
                    Row.of("Beijing",
                            new BigDecimal("39.9050"),
                            new BigDecimal("116.3914"),
                                    "China","CN","Beijing","primary",19433000L)
                };

        writeFile(testDir, "data.csv", csvLines);

        DataType dataType =
                ROW(
                        FIELD("city", STRING()),
                        FIELD("lat", DECIMAL(8, 4)),
                        FIELD("lng", DECIMAL(8, 4)),
                        FIELD("country", STRING()),
                        FIELD("iso2", STRING()),
                        FIELD("admin_name", STRING()),
                        FIELD("capital", STRING()),
                        FIELD("population", BIGINT()));
        RowType rowType = (RowType) dataType.getLogicalType();

        CsvRowDataDeserializationSchema deserSchema =
                new CsvRowDataDeserializationSchema.Builder(rowType, InternalTypeInfo.of(rowType))
                        .build();

        DelimitedFormat<RowData> csvFormat = DelimitedFormat.of("\n", deserSchema);

        final List<RowData> result = initializeSourceAndReadData(testDir, csvFormat);

        verifyResult(dataType, expected, result);
    }

    private List<RowData> initializeSourceAndReadData(
            File testDir, DelimitedFormat<RowData> csvFormat) throws Exception {
        final FileSource<RowData> source =
                FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(testDir)).build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        final DataStream<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        final ClientAndIterator<RowData> client =
                DataStreamUtils.collectWithClient(stream, "Bounded TextFiles Test");

        final List<RowData> result = new ArrayList<>();
        while (client.iterator.hasNext()) {
            RowData next = client.iterator.next();
            result.add(next);
        }
        return result;
    }

    // ------------------------------------------------------------------------
    //  verification
    // ------------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void verifyResult(DataType dataType, Row[] expected, List<RowData> result) {

        DataFormatConverters.DataFormatConverter converterForDataType =
                DataFormatConverters.getConverterForDataType(dataType);
        List<Row> actualRows =
                result.stream()
                        .map(e -> (Row) converterForDataType.toExternal(e))
                        .collect(Collectors.toList());
        ;

        assertEquals(Arrays.asList(expected), actualRows);
    }

    // ------------------------------------------------------------------------
    //  test data
    // ------------------------------------------------------------------------

    private static void writeFile(File testDir, String fileName, String[] lines)
            throws IOException {
        final File file = new File(testDir, fileName);
        writeFileAtomically(file, lines);
    }

    private static void writeFileAtomically(final File file, final String[] lines)
            throws IOException {
        writeFileAtomically(file, lines, (v) -> v);
    }

    private static void writeFileAtomically(
            final File file,
            final String[] lines,
            final FunctionWithException<OutputStream, OutputStream, IOException>
                    streamEncoderFactory)
            throws IOException {

        // we don't use TMP_FOLDER.newFile() here because we don't want this to actually create a
        // file,
        // but just construct the file path
        final File stagingFile =
                new File(TMP_FOLDER.getRoot(), ".tmp-" + UUID.randomUUID().toString());

        try (final FileOutputStream fileOut = new FileOutputStream(stagingFile);
                final OutputStream out = streamEncoderFactory.apply(fileOut);
                final OutputStreamWriter encoder =
                        new OutputStreamWriter(out, StandardCharsets.UTF_8);
                final PrintWriter writer = new PrintWriter(encoder)) {

            for (String line : lines) {
                writer.println(line);
            }
        }

        final File parent = file.getParentFile();
        assertTrue(parent.mkdirs() || parent.exists());

        assertTrue(stagingFile.renameTo(file));
    }
}

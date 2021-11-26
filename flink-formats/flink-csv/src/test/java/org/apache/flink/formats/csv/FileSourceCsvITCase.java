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
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

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
import java.util.Objects;
import java.util.UUID;

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
    @Test
    public void testBoundedTextFileSourceWithJackson() throws Exception {
        final File testDir = TMP_FOLDER.newFolder();
        writeFile(testDir, "data.csv", CSV_LINES);

        final CsvReaderFormat<CitiesPojo> csvFormat = CsvReaderFormat.forPojo(CitiesPojo.class);
        final List<CitiesPojo> result = initializeSourceAndReadData(testDir, csvFormat);

        final CitiesPojo[] expected =
                new CitiesPojo[] {
                    new CitiesPojo(
                            "Berlin",
                            new BigDecimal("52.5167"),
                            new BigDecimal("13.3833"),
                            "Germany",
                            "DE",
                            "Berlin",
                            "primary",
                            3644826L),
                    new CitiesPojo(
                            "San Francisco",
                            new BigDecimal("37.7562"),
                            new BigDecimal("-122.4430"),
                            "United States",
                            "US",
                            "California",
                            "",
                            3592294L),
                    new CitiesPojo(
                            "Beijing",
                            new BigDecimal("39.9050"),
                            new BigDecimal("116.3914"),
                            "China",
                            "CN",
                            "Beijing",
                            "primary",
                            19433000L)
                };

        assertEquals(Arrays.asList(expected), result);
    }

    @JsonPropertyOrder({
        "city",
        "lat",
        "lng",
        "country",
        "iso2",
        "adminName",
        "capital",
        "population"
    })
    public static class CitiesPojo {
        public String city;
        public BigDecimal lat;
        public BigDecimal lng;
        public String country;
        public String iso2;
        public String adminName;
        public String capital;
        public long population;

        public CitiesPojo() {};

        public CitiesPojo(
                String city,
                BigDecimal lat,
                BigDecimal lng,
                String country,
                String iso2,
                String admin_name,
                String capital,
                long population) {
            this.city = city;
            this.lat = lat;
            this.lng = lng;
            this.country = country;
            this.iso2 = iso2;
            this.adminName = admin_name;
            this.capital = capital;
            this.population = population;
        }

        @Override
        public String toString() {
            return "CitiesPojo{"
                    + "city='"
                    + city
                    + '\''
                    + ", lat="
                    + lat
                    + ", lng="
                    + lng
                    + ", country='"
                    + country
                    + '\''
                    + ", iso2='"
                    + iso2
                    + '\''
                    + ", adminName='"
                    + adminName
                    + '\''
                    + ", capital='"
                    + capital
                    + '\''
                    + ", population="
                    + population
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CitiesPojo that = (CitiesPojo) o;
            return population == that.population
                    && Objects.equals(city, that.city)
                    && Objects.equals(lat, that.lat)
                    && Objects.equals(lng, that.lng)
                    && Objects.equals(country, that.country)
                    && Objects.equals(iso2, that.iso2)
                    && Objects.equals(adminName, that.adminName)
                    && Objects.equals(capital, that.capital);
        }

        @Override
        public int hashCode() {
            return Objects.hash(city, lat, lng, country, iso2, adminName, capital, population);
        }
    }

    private static <T> List<T> initializeSourceAndReadData(File testDir, StreamFormat<T> csvFormat)
            throws Exception {
        final FileSource<T> source =
                FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(testDir)).build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        final DataStream<T> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        final ClientAndIterator<T> client =
                DataStreamUtils.collectWithClient(stream, "Bounded TextFiles Test");

        final List<T> result = new ArrayList<>();
        while (client.iterator.hasNext()) {
            T next = client.iterator.next();
            result.add(next);
        }
        return result;
    }

    // ------------------------------------------------------------------------
    //  test data
    // ------------------------------------------------------------------------

    private static final String[] CSV_LINES =
            new String[] {
                "Berlin,52.5167,13.3833,Germany,DE,Berlin,primary,3644826",
                "San Francisco,37.7562,-122.4430,United States,US,California,,3592294",
                "Beijing,39.9050,116.3914,China,CN,Beijing,primary,19433000"
            };

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

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

import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.connector.upserttest.sink.UpsertTestFileUtil;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.util.DockerImageVersions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** E2E Test for SqlClient. */
public class SqlClientITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlClientITCase.class);

    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);
    private static final Path sqlToolBoxJar = TestUtils.getResource(".*SqlToolbox.jar");

    private final Path sqlConnectorKafkaJar = TestUtils.getResource(".*kafka.*.jar");

    private final Path sqlConnectorUpsertTestJar =
            TestUtils.getResource(".*flink-test-utils.*.jar");

    public static final Network NETWORK = Network.newNetwork();

    @Container
    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("kafka")
                    .withLogConsumer(LOG_CONSUMER);

    private final FlinkContainersSettings flinkContainersSettings =
            FlinkContainersSettings.builder().numTaskManagers(1).build();
    private final TestcontainersSettings testcontainersSettings =
            TestcontainersSettings.builder().network(NETWORK).logger(LOG).dependsOn(KAFKA).build();

    public final FlinkContainers flink =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(flinkContainersSettings)
                    .withTestcontainersSettings(testcontainersSettings)
                    .build();

    @TempDir private File tempDir;

    @BeforeEach
    void setup() throws Exception {
        flink.start();
    }

    @AfterEach
    void tearDown() {
        flink.stop();
    }

    @Test
    void testUpsert() throws Exception {
        String outputFilepath = "/tmp/records-upsert.out";

        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.runtime-mode' = 'batch';",
                        "",
                        "CREATE TABLE UpsertSinkTable (",
                        "    user_id INT,",
                        "    user_name STRING,",
                        "    user_count BIGINT,",
                        "    PRIMARY KEY (user_id) NOT ENFORCED",
                        "  ) WITH (",
                        "    'connector' = 'upsert-files',",
                        "    'key.format' = 'json',",
                        "    'value.format' = 'json',",
                        "    'output-filepath' = '" + outputFilepath + "'",
                        "  );",
                        "",
                        "INSERT INTO UpsertSinkTable(",
                        "  SELECT user_id, user_name, COUNT(*) AS user_count",
                        "  FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), (42, 'Kim'), (42, 'Kim'), (1, 'Bob'))",
                        "    AS UserCountTable(user_id, user_name)",
                        "  GROUP BY user_id, user_name);");
        executeSql(sqlLines);
        sqlLines =
                Arrays.asList(
                        "SET 'execution.runtime-mode' = 'streaming';",
                        "SET 'sql-client.execution.result-mode' = 'tableau';",
                        "SELECT * FROM UpsertSinkTable");

        executeSql(sqlLines);

        /*
        | user_id | user_name | user_count |
        | ------- | --------- | ---------- |
        |    1    |    Bob    |     2      |
        |   22    |    Tom    |     1      |
        |   42    |    Kim    |     3      |
        */

        verifyNumberOfResultRecords(outputFilepath, 3);
    }

    @Test
    void testUpsertFromKafka() throws Exception {
        String outputFilepath = "/tmp/records-upsert.out";

        String[] messages =
                new String[] {
                    "{\"timestamp\": \"2018-03-12T08:00:00Z\", \"user\": \"Alice\" }",
                    "{\"timestamp\": \"2018-03-12T09:00:00Z\", \"user\": \"Bob\" }",
                    "{\"timestamp\": \"2018-03-12T09:20:00Z\", \"user\": \"Steve\" }"
                };
        sendMessages("test-json", messages);

        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.runtime-mode' = 'batch';",
                        "",
                        "CREATE FUNCTION RegReplace AS 'org.apache.flink.table.toolbox.StringRegexReplaceFunction';",
                        "",
                        "CREATE TABLE JsonSourceTable (",
                        "    `timestamp` TIMESTAMP_LTZ(3),",
                        "    `user` STRING,",
                        "    `rowtime` AS `timestamp`,",
                        "    WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '2' SECOND",
                        ") WITH (",
                        "    'connector' = 'kafka',",
                        "    'topic' = 'test-json',",
                        "    'properties.bootstrap.servers' = '"
                                + KAFKA.getBootstrapServers()
                                + "',",
                        "    'scan.startup.mode' = 'earliest-offset',",
                        "    'format' = 'json',",
                        "    'json.timestamp-format.standard' = 'ISO-8601'",
                        ");",
                        "",
                        "CREATE TABLE AppendSinkTable (",
                        "    user_name STRING",
                        "  ) WITH (",
                        "    'connector' = 'upsert-files',",
                        "    'key.format' = 'json',",
                        "    'value.format' = 'json',",
                        "    'output-filepath' = '" + outputFilepath + "'",
                        "  );");

        executeSql(sqlLines);

        sqlLines =
                Arrays.asList(
                        "INSERT INTO AppendSinkTable",
                        "    SELECT \\`user\\` as `user_name`",
                        "    FROM JsonSourceTable");
        executeSql(sqlLines);

        verifyNumberOfResultRecords(outputFilepath, 3);
    }

    @Test
    void testUpsertFromKafkaPassthrough() throws Exception {

        //        flink.getJobManager()
        //                .copyFileToContainer(MountableFile.forHostPath(sqlConnectorKafkaJar),
        // "/opt/flink");

        flink.getTaskManagers()
                .forEach(
                        c ->
                                c.copyFileToContainer(
                                        MountableFile.forHostPath(sqlConnectorKafkaJar),
                                        "/tmp/kafka-sql.jar"));

        String outputFilepath = "/tmp/records-upsert.out";

        String[] messages =
                new String[] {
                    "{\"user_name\": \"Alice\" }",
                    "{\"user_name\": \"Bob\" }",
                    "{\"user_name\": \"Steve\" }"
                };
        sendMessages("test-json", messages);

        List<String> sqlLines =
                Arrays.asList(
                        //                        "SET 'execution.runtime-mode' = 'batch';",
                        //                        "SET 'sql-client.verbose' = 'true'",
                        //                        "",
                        "CREATE TABLE JsonSourceTable (",
                        "    user_name STRING",
                        " ) WITH (",
                        "    'connector' = 'kafka',",
                        "    'topic' = 'test-json',",
                        "    'properties.bootstrap.servers' = '"
                                + formatKafkaBootstrapServers()
                                + "',",
                        "    'scan.startup.mode' = 'earliest-offset',",
                        "    'format' = 'json'",
                        ");",
                        "",
                        "CREATE TABLE AppendSinkTable (",
                        "    user_name STRING,",
                        "    PRIMARY KEY (user_name) NOT ENFORCED",
                        "  ) WITH (",
                        "    'connector' = 'upsert-files',",
                        "    'key.format' = 'json',",
                        "    'value.format' = 'json',",
                        "    'output-filepath' = '" + outputFilepath + "'",
                        "  );");
        executeSql(sqlLines);
        //        flink.submitJob();

        sqlLines =
                Arrays.asList(
                        "INSERT INTO AppendSinkTable",
                        "    SELECT `user_name`",
                        "    FROM JsonSourceTable");
        executeSql(sqlLines);

        verifyNumberOfResultRecords(outputFilepath, 3);
    }

    @Test
    void testMatchRecognize() throws Exception {
        String outputFilepath = "/tmp/records-matchrecognize.out";

        String[] messages =
                new String[] {
                    "{\"timestamp\": \"2018-03-12T08:00:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                    "{\"timestamp\": \"2018-03-12T08:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                    "{\"timestamp\": \"2018-03-12T09:00:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is another warning.\"}}",
                    "{\"timestamp\": \"2018-03-12T09:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"INFO\", \"message\": \"This is a info.\"}}",
                    "{\"timestamp\": \"2018-03-12T09:20:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                    "{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                    "{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": null, \"event\": { \"type\": \"WARNING\", \"message\": \"This is a bad message because the user is missing.\"}}",
                    "{\"timestamp\": \"2018-03-12T10:40:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"ERROR\", \"message\": \"This is an error.\"}}"
                };
        sendMessages("test-json", messages);

        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.runtime-mode' = 'batch';",
                        "",
                        "CREATE FUNCTION RegReplace AS 'org.apache.flink.table.toolbox.StringRegexReplaceFunction';",
                        "",
                        "CREATE TABLE JsonSourceTable (",
                        "    `timestamp` TIMESTAMP_LTZ(3),",
                        "    `user` STRING,",
                        "    `event` ROW<type STRING, message STRING>,",
                        "    `rowtime` AS `timestamp`,",
                        "    WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '2' SECOND",
                        ") WITH (",
                        "    'connector' = 'kafka',",
                        "    'topic' = 'test-json',",
                        "    'properties.bootstrap.servers' = '"
                                //                                + formatKafkaBootstrapServers()
                                + "bla:1234"
                                + "',",
                        "    'scan.startup.mode' = 'earliest-offset',",
                        "    'format' = 'json',",
                        "    'json.timestamp-format.standard' = 'ISO-8601'",
                        ");",
                        "",
                        "CREATE TABLE AppendSinkTable (",
                        "    user_id INT,",
                        "    user_name STRING,",
                        "    user_count BIGINT",
                        "  ) WITH (",
                        "    'connector' = 'upsert-files',",
                        "    'key.format' = 'json',",
                        "    'value.format' = 'json',",
                        "    'output-filepath' = '" + outputFilepath + "'",
                        "  );");
        executeSql(sqlLines);

        sqlLines =
                Arrays.asList(
                        "INSERT INTO AppendSinkTable",
                        "  SELECT 1 as user_id, T.userName as user_name, cast(1 as BIGINT) as user_count",
                        "  FROM (",
                        "    SELECT \\`user\\`, \\`rowtime\\`",
                        "    FROM JsonSourceTable",
                        "    WHERE \\`user\\` IS NOT NULL)",
                        "  MATCH_RECOGNIZE (",
                        "    ORDER BY rowtime",
                        "    MEASURES",
                        "        \\`user\\` as userName",
                        "    PATTERN (A)",
                        "    DEFINE",
                        "        A as \\`user\\` = 'Alice'",
                        "  ) T");
        executeSql(sqlLines);

        verifyNumberOfResultRecords(outputFilepath, 3);
    }

    public void sendMessages(String topic, String... messages) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (Producer<Bytes, String> producer =
                new KafkaProducer<>(props, new BytesSerializer(), new StringSerializer())) {
            for (String message : messages) {
                producer.send(new ProducerRecord<>(topic, message));
            }
        }
    }

    private void verifyNumberOfResultRecords(String resultFilePath, int expectedNumberOfRecords)
            throws IOException, InterruptedException {
        File tempOutputFile = new File(tempDir, "records.out");
        String tempOutputFilepath = tempOutputFile.toString();
        GenericContainer<?> taskManager = flink.getTaskManagers().get(0);
        Thread.sleep(5000); // prevent NotFoundException: Status 404
        taskManager.copyFileFromContainer(resultFilePath, tempOutputFilepath);

        int numberOfResultRecords = UpsertTestFileUtil.getNumberOfRecords(tempOutputFile);
        assertThat(numberOfResultRecords).isEqualTo(expectedNumberOfRecords);
    }

    private void executeSql(List<String> sqlLines) throws Exception {
        flink.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorUpsertTestJar, sqlConnectorKafkaJar, sqlToolBoxJar)
                        .build());
    }

    private static String formatKafkaBootstrapServers() {
        return String.join(
                ",",
                KAFKA.getBootstrapServers(),
                KAFKA.getNetworkAliases().stream()
                        .map(host -> String.join(":", host, Integer.toString(9092)))
                        .collect(Collectors.joining(",")));
    }
}

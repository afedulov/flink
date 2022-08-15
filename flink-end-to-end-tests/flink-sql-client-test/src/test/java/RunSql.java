package com.ververica.playground.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class RunSql {
    public static void main(String[] args) {
        //        StreamExecutionEnvironment env =
        // StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment localEnv =
                StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8081);
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnv.executeSql(
                "CREATE TABLE test1 (\n"
                        + "    `user` STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'kafka',\n"
                        + "    'topic' = 'test',\n"
                        + "    'properties.bootstrap.servers' = 'localhost:9092',\n"
                        + "    'scan.startup.mode' = 'earliest-offset',\n"
                        + "    'format' = 'json',\n"
                        + "    'json.timestamp-format.standard' = 'ISO-8601'\n"
                        + ");");
    }
}

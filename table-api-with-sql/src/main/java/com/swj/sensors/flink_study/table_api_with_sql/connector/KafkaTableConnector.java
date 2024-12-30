package com.swj.sensors.flink_study.table_api_with_sql.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/26 22:22
 */
public class KafkaTableConnector {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

    // 1.11 的版本，前面还都得要带上 connector.
    String ddlSql = "CREATE TABLE kafka_click_event_table (\n"
        + "`userName` STRING,\n"
        + "`visitUrl` STRING, \n"
        + "`ts` STRING \n" // 从 kafka 的元数据中读取 timestamp 信息，这个要 1.13 版本才可以，目前我是 1.11 版本，就继续改用 string
        + ") WITH (\n"
        + " 'connector.type' = 'kafka', \n"
        + " 'connector.version' = '0.11', \n" // kafka 的版本，Flink附带了提供了多个Kafka连接器：universal通用版本，0.10，0.11，官方文档解释说universal(通用版本)的连接器，会尝试跟踪Kafka最新版本，兼容0.10或者之后的Kafka版本，官方文档也说对于绝大多数情况使用这个即可
        + " 'connector.topic' = 'click_events', \n"
        + " 'connector.properties.bootstrap.servers' = 'localhost:9092', \n"
        + " 'connector.properties.group.id' = 'kafka-table-connector-group-1', \n"
        + " 'connector.startup-mode' = 'earliest-offset', \n"
        + " 'update-mode' = 'append', \n" // declare update mode
        + " 'format.type' = 'json' \n"
        + " )";

    // create table connected to kafka
    TableResult tableResult = tEnv.executeSql(ddlSql);
    String path = "/tmp/kafka_click_events_sink_result.csv";
    CsvTableSink sink = new CsvTableSink(path,
        "|",
        1,
        FileSystem.WriteMode.OVERWRITE);

    tEnv.registerTableSink("result_table",
        // specify the table schema
        new String[] {"userName", "visitUrl", "ts"},
        new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING},
        sink);

    tEnv.sqlUpdate("insert into result_table(userName,visitUrl,ts) select * from kafka_click_event_table");
    tEnv.execute("Kafka Table Connector Test");
  }
}

package com.swj.sensors.flink_study.table_api_with_sql;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/24 15:50
 */
public class RowRegisterRowWithNames {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    List<Row> rowList = new ArrayList<>();
    rowList.add(Row.of(1, 1L, "Hi"));
    rowList.add(Row.of(2, 2L, "Hello"));
    rowList.add(Row.of(3, 3L, "Hello, World"));

    TypeInformation<?>[] types = {
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
    };

    String[] fieldNames = {"a", "b", "c"};

    RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

    DataStream<Row> rowStream = env.fromCollection(rowList).returns(rowTypeInfo);

    Table table = tableEnv.fromDataStream(rowStream,"a,b,c");

    // 先注册，再查询
    tableEnv.registerTable("MyTableRow",table);

    String sql="select a,c from MyTableRow";

    Table queryTable = tableEnv.sqlQuery(sql);

    DataStream<Row> rowQueryResult = tableEnv.toAppendStream(queryTable, Row.class);

    rowQueryResult.print();

    env.execute(RowRegisterRowWithNames.class.getSimpleName());
  }
}

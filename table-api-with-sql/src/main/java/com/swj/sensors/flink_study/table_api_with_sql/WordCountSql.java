package com.swj.sensors.flink_study.table_api_with_sql;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/24 11:38
 */
public class WordCountSql {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    /**
     * TableEnvironment 是 Table API 和 Sql 的核心概念。它负责：
     * 在内部的 catalog 中注册 Table
     * 注册外部的 catalog
     * 加载可插拔模块
     * 执行 Sql 查询
     * 注册自定义函数(scala,table 或者 aggregation)
     * DataStream 和 Table 之间的转换(面向 StreamTableEnvironment)
     Table 总是与特定的 TableEnvironment 绑定。不能在同一条查询中使用不同 TableEnvironment 中的表，比如在 join 或者 union
     *
     */
    BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

    DataSet<WC> dataSet = env.fromElements(
        new WC("hello", 1),
        new WC("flink", 1),
        new WC("hello", 1)
    );

    // convert DataSet to Table
//    Table table = tEnv.fromDataSet(dataSet);
//    // 在内部的 catalog 中注册 table
//    tEnv.registerTable("WordCount", table);

    // register DataSet as Table from env
    tEnv.registerDataSet("WordCount", dataSet, "word, frequency");

    // run a sql query on he table and retrieve the result as a new Table
    Table table = tEnv.sqlQuery("SELECT word, sum(frequency) as frequency FROM WordCount GROUP BY word");

    Table filterTable = table.filter("frequency = 2");

    DataSet<WC> result = tEnv.toDataSet(filterTable, WC.class);

    result.print();

  }

  @AllArgsConstructor
  @NoArgsConstructor
  public static class WC {
    public String word;
    public int frequency;

    @Override
    public String toString() {
      return "WC{" +
          "word='" + word + '\'' +
          ", frequency=" + frequency +
          '}';
    }
  }
}

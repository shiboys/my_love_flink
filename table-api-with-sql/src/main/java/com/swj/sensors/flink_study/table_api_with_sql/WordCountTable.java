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
 * @since 2023/12/24 15:19
 */
public class WordCountTable {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
    DataSet<WC> dataSet = env.fromElements(
        new WC("Hello", 1),
        new WC("Flink", 1),
        new WC("Hello", 1)
    );

    Table table = tEnv.fromDataSet(dataSet);
    tEnv.registerTable("WordCount", table);

    Table filterTable = tEnv.scan("WordCount")
        .groupBy("word")
        .select("word, frequency.sum as frequency") // 纳尼？ 不应该是 select("word","count(frequency) as frequency") 吗？
        .filter("frequency = 2");
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

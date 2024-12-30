package com.swj.sensors.flink_study.table_api_with_sql.sql;

import static org.apache.flink.table.api.Expressions.$;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/26 18:39
 */
public class UnionTableExample {


  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    DataStream<Order> orderStreamA = env.fromElements(
        new Order(1L, "beer", 3),
        new Order(1L, "diaper", 4), // diaper：尿片
        new Order(3L, "beer", 2)
    );

    DataStream<Order> orderStreamB = env.fromElements(
        new Order(2L, "pen", 3),
        new Order(2L, "rubber", 3),
        new Order(4L, "beer", 1)
    );

    // 通过 fromDataStream 将 DataStream 转换成 Table
    Table tableA = tEnv.fromDataStream(orderStreamA, $("userId"), $("product"), $("amount"));

    // 通过 registerDataStream 方法将 DataStream 转换成 table
    tEnv.registerDataStream("tableB", orderStreamB, "userId,product,amount");

    //union the two table

    String sql = "SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL "
        + "SELECT * FROM tableB WHERE amount < 2";

    Table table = tEnv.sqlQuery(sql);

    // 直接 用 Order.class 反射
    tEnv.toAppendStream(table, Order.class).print();

    env.execute();
  }

  @AllArgsConstructor
  @NoArgsConstructor
  public static class Order {
    public Long userId;
    public String product;
    public int amount;


    @Override
    public String toString() {
      return "Order{" +
          "userId=" + userId +
          ", product='" + product + '\'' +
          ", amount=" + amount +
          '}';
    }
  }
}

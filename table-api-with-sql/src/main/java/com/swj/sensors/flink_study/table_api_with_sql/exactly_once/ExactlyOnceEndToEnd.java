package com.swj.sensors.flink_study.table_api_with_sql.exactly_once;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2024/02/05 19:16
 */
public class ExactlyOnceEndToEnd {

//  static {
//    String driverName = "com.mysql.cj.jdbc.Driver";
//    try {
//      Class.forName(driverName);
//    } catch (ClassNotFoundException e) {
//      e.printStackTrace();
//    }
//  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    String checkpointFilePath = "file:///tmp/flink_checkpoint";
    env.setParallelism(1);// 测试使用
    env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
    env.setStateBackend(new FsStateBackend(checkpointFilePath));


    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink");
    String topic = "exactly-once-topic";

    FlinkKafkaConsumer011<ObjectNode> kafkaSource =
        new FlinkKafkaConsumer011<>(topic, new JSONKeyValueDeserializationSchema(true), props);
    //从 groupOffset 开始消费，如果 kafka broker 端没有该 group 的信息，则从 "auto_offset_reset" 配置项指定的值开始
    kafkaSource.setStartFromGroupOffsets();
    // flink 执行 checkpoint 的时候，提交偏移量。(一份在 Flink 的 checkpoint 中，一份在 kafka 的 __customer_offsets 中
    kafkaSource.setCommitOffsetsOnCheckpoints(true);

    DataStreamSource<ObjectNode> kafkaStream = env.addSource(kafkaSource);

    // transformation

    // sink
    kafkaStream.addSink(new MySqlTwoPhaseCommitSinkFunction()).name("mysql2PhaseCommitSink");
    // execute
    env.execute();
  }

  private static void testJdbc() {
    /**
     * create table t_exactly_once(id bigint not null auto_increment,
     * value varchar(255) default null,
     * insert_time datetime default null,
     * primary key(id) )
     * engine=Innodb default charset=utf8mb4;
     */
    Random random = new Random();
    String value = String.valueOf(random.nextInt(100));
    try {
      addExactOneInstance(value, true);
      System.out.println("done");
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }

  }

  static void addExactOneInstance(String value, boolean manualCommit) throws SQLException {
    String sql = "insert into t_exactly_once(value,insert_time) values (?,?); ";
    Connection conn = getDefaultConnection();
    PreparedStatement preparedStatement = conn.prepareStatement(sql);
    preparedStatement.setString(1, value);
    preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
    preparedStatement.execute();
    if (manualCommit) {
      DbConnectionUtil.commit(conn);
    }
  }



  private static Connection getDefaultConnection() {
    String url = "jdbc:mysql://localhost:3306/flink?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    String user = "flink";
    String password = "123456";
    return DbConnectionUtil.getConnection(url, user, password);
  }

  static class MySqlTwoPhaseCommitSinkFunction extends TwoPhaseCommitSinkFunction<ObjectNode, Connection, Void> {


    public MySqlTwoPhaseCommitSinkFunction() {
      // 父类构造函数要求必须传入 事务的序列化器和上下文的序列化器
      // 这里的序列化器要序列化 Jdbc 的 Connection 对象，提交之后，flink 序列化失败
      // 在看了 FlinkKafkaProducer011 的源码之后(因为它也是继承 TwoPhaseCommitSinkFunction )，发现它没有序列化 KafakProducer
      // 而是在每次反序列化的时候重新创建，但是 Kafka producer 重建之后可以继续提交之前的事务的前提是，能拿到 kafka 的 transactionId
      // mysql 的 connection 无法序列化，我也想通过将 connectionId 或者 transactionId 之类的序列化到 checkpoint 但是目前来看，找不到可行的办法
      // 因此这个自定义的两阶段提交以失败告终。
      // KryoSerializer 无法序列化 Connection，Mysql Jdbc Connection 的实现类，不允许被 Kyro 序列化。
      super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(Connection transaction, ObjectNode value, Context context) throws Exception {
      System.out.println("begin to invoke ...");
      // 向 mysql 中添加一条记录，但是不提交
      String valStr = value.get("value").toString();
      addExactOneInstance(valStr, false);

      // 模拟出现异常， 测试两阶段提价的异常情况
      if (Objects.equals(valStr, "15")) {
        System.out.println(1 / 0);
      }
    }

    @Override
    protected Connection beginTransaction() throws Exception {
      // 通过 Connection.setAutoCommit(false) 将 自动提交事务取消
      // mysql 的自动提交取消之后，只能通过手动提交或者回滚
      return getDefaultConnection();
    }

    @Override
    protected void preCommit(Connection transaction) throws Exception {
      // 预提交, 因为预提交的内容已经在 invoke 中实现了，这里就是空的方法体
      System.out.println("start to preCommit ..." + transaction);
    }

    @Override
    protected void commit(Connection transaction) {
      System.out.println("start to commit..");
      DbConnectionUtil.commit(transaction);
    }

    @Override
    protected void abort(Connection transaction) {
      System.out.println("start to abort, so rollback the transaction");
      DbConnectionUtil.rollback(transaction);
    }
  }


  static class DbConnectionUtil {
    static Connection getConnection(String url, String user, String password) {
      Connection conn = null;
      try {
        conn = DriverManager.getConnection(url, user, password);
        // 关闭自动提交事务
        conn.setAutoCommit(false);
      } catch (SQLException throwables) {
        throwables.printStackTrace();
      }
      return conn;
    }

    static void commit(Connection conn) {
      try {
        conn.commit();
      } catch (SQLException throwables) {
        throwables.printStackTrace();
      }
    }

    static void rollback(Connection conn) {
      try {
        conn.rollback();
      } catch (SQLException throwables) {
        throwables.printStackTrace();
      }
    }

    static void close(Connection conn) {
      try {
        conn.close();
      } catch (SQLException throwables) {
        throwables.printStackTrace();
      }
    }
  }


}

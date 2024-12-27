package com.swj.sensors.flink_study.streamapi.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 15:56
 */
public class StreamingKafkaSource {
  /**
   * 连接 kafka 的数据源形成的流
   * todo:启动本机kafka 进行测试
   */
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 设置 checkpoint 信息

    env.enableCheckpointing(5000);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointTimeout(60_000);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    // job 取消时，保留在外部的 checkpoint
    //  你可以配置周期存储 checkpoint 到外部系统中。Externalized checkpoints 将他们的元数据写到持久化存储上并且在 job 失败的时候不会被自动删除。
    //  这种方式下，如果你的 job 失败，你将会有一个现有的 checkpoint 去恢复。
    env.getCheckpointConfig().enableExternalizedCheckpoints(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    // 连续三次 checkpoint 失败就退出。该属性定义可容忍多少次连续的 checkpoint 失败。超过这个阈值之后会触发作业错误 fail over
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);


    String groupJobName = StreamingKafkaSource.class.getSimpleName();
    String topic = "flink-kafka-source-sink3";
    String groupId = groupJobName;
    Properties properties = new Properties();
    properties.put("group.id", groupId);
    properties.put("bootstrap.servers", "localhost:9092");

//    FlinkKafkaConsumer011<String> consumer =
//        new FlinkKafkaConsumer011<>(topic, new KafkaDeserializationSchemaWrapper<>(new SimpleStringSchema()),
//            properties);

    FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);
    // 默认的从 groupoffset 读取，也就是 kafka 的内置 topic --consumer-offset 里面去取有关该 group 的消费 offset 信息
    consumer.setStartFromGroupOffsets();

    DataStreamSource<String> textStream = env.addSource(consumer);
    textStream.print().setParallelism(1);
    env.execute(groupJobName);
  }
}

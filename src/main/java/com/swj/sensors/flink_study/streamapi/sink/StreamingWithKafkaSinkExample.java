package com.swj.sensors.flink_study.streamapi.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/01 12:19
 */
public class StreamingWithKafkaSinkExample {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // checkpoint 的配置
    env.enableCheckpointing(5000);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    env.getCheckpointConfig().setCheckpointTimeout(60_000);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    // 保留外部的 RETAIN_ON_CANCELLATION
    env.getCheckpointConfig().enableExternalizedCheckpoints(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    // 设置 statebackend
    //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop001:9000/com.mylove.flink/checkpoints",true))

    DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9000, "\n");

    String brokerUrl = "localhost:9092";
    String topic = "flink-kafka-source-sink3";
    Properties properties = new Properties();
    properties.put("bootstrap.servers", brokerUrl);
    // 使用 Exactly_ONCE 语义的 KafkaProducer
    // 要想使用这个语义，需要kafka 事务支持
    // 这里面的需要配置事务超时时间，一个是 producer 端的参数 transaction.timeout.ms,默认为 60 秒
    // 另外一个是 Broker 端参数 transaction.max.timeout.ms ，默认15 分钟
//    FlinkKafkaProducer011<String> kafkaProducer011 =
//        new FlinkKafkaProducer011<>(topic, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), properties,
//            FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
    /**
     * 上面的配置方式会报异常
     * org.apache.kafka.common.KafkaException: org.apache.kafka.common.serialization.ByteArraySerializer is not an instance of org.apache.kafka.common.seri
     * alization.Serializer
     * 具体没有深究
     */

    FlinkKafkaProducer011<String> kafkaProducer011 =
        new FlinkKafkaProducer011<>(topic, new SimpleStringSchema(), properties);
    socketTextStream.addSink(kafkaProducer011);

    env.execute("StreamingWithKafkaSinkExample");
  }
}

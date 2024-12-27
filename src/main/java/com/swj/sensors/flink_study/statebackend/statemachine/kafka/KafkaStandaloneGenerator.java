package com.swj.sensors.flink_study.statebackend.statemachine.kafka;

import com.swj.sensors.flink_study.statebackend.statemachine.event.Event;
import com.swj.sensors.flink_study.statebackend.statemachine.generataor.StandaloneThreadedGenerator;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/11 23:14
 * 该类的主要作用是实例化 collector ，然后按照主题分区-线程一一映射的方式
 * 来调用 StandaloneThreadedGenerator.runCollector ，将 generator 产生的数据
 * 通过 Kafka Producer 发送到 Kafka Broker。
 * 这个类有 Main 方法， 意味着这个类可以独立运行，不依赖 Flink 框架
 */
public class KafkaStandaloneGenerator {
  public static final String TOPIC = "flink-generator-demo-topic-1";
  private static final String BROKER_URLS = "localhost:9092";
  private static final String GENERATOR_KEY = "Standalone-Generator";
  // 分区个数
  private static final int PARTITIONS_COUNT = 1;

  public static void main(String[] args) throws Exception {
    // 一个分区一个 collector 的生成器
    KafkaCollector[] collectors = new KafkaCollector[PARTITIONS_COUNT];
    for (int i = 0; i < collectors.length; i++) {
      collectors[i] = new KafkaCollector(BROKER_URLS, TOPIC, i);
    }
    StandaloneThreadedGenerator.runCollector(collectors);
  }

  private static class KafkaCollector implements Collector<Event> {

    private KafkaProducer<byte[], byte[]> producer;
    EventDeserializer eventDeserializer = new EventDeserializer();
    private String topic;
    private int partition;

    public KafkaCollector(String brokers, String topic, int partition) {
      this.topic = topic;
      this.partition = partition;

      Properties properties = new Properties();
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
      producer = new KafkaProducer<>(properties);
    }


    @Override
    public void collect(Event record) {
      byte[] serialized = eventDeserializer.serialize(record);
      producer.send(new ProducerRecord<>(topic, partition, GENERATOR_KEY.getBytes(), serialized));
    }

    @Override
    public void close() {
      producer.close();
    }
  }

}

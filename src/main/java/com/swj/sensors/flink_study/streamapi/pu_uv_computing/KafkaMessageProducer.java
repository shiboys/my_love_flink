package com.swj.sensors.flink_study.streamapi.pu_uv_computing;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/05 11:27
 */
public class KafkaMessageProducer {

  private static final Random randomId = new Random();
  private static final Random randomCat = new Random();
  private static final String topic = "flink-kafka-source-sink";
  private static final int MESSAGE_BATCH_SIZE = 10_000;

  private static final String[] CATEGORIES = new String[] {"c1", "c2", "c3", "c4"};
  private static final String[] ACTIONS = new String[] {"click", "buy", "login", "logout"};

  static void produceMessage() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_PRODUCER_URL);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    int counter = 0;
    while (counter < MESSAGE_BATCH_SIZE) {
      //String msg = generateMessage();
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, generateMessage());
      producer.send(record);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      counter++;
    }
  }

  static String generateMessage() {
    UserBehaviorEvent event = new UserBehaviorEvent();
    event.setUserId(randomId.nextInt(MESSAGE_BATCH_SIZE));
    event.setItemId(randomId.nextInt(MESSAGE_BATCH_SIZE));
    event.setCategoryId(CATEGORIES[randomCat.nextInt(CATEGORIES.length)]);
    event.setAction(ACTIONS[randomCat.nextInt(ACTIONS.length)]);
    event.setTs(System.currentTimeMillis());
    String ser = JSON.toJSONString(event);
    System.out.println(ser);
    return ser;
  }

  public static void main(String[] args) {
    produceMessage();
  }
}

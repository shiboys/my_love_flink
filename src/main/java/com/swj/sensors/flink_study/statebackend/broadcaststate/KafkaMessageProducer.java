package com.swj.sensors.flink_study.statebackend.broadcaststate;

import com.alibaba.fastjson.JSON;
import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Action;
import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Pattern;
import com.swj.sensors.flink_study.streamapi.pu_uv_computing.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/05 11:27
 */
public class KafkaMessageProducer {

  private static final Random randomId = new Random();
  private static final Random randomAction = new Random();

  private static final String DEFAULT_TOPIC = "action-topic";
  private static final String PATTERN_TOPIC = "pattern-topic";
  private static final int MESSAGE_BATCH_SIZE = 10_000;

  private static List<Pattern> patterns;
  private static final String[] ACTIONS = new String[] {"login", "click", "buy", "logout", "drop"};

  static void produceMessage(String topic) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_PRODUCER_URL);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    int counter = 0;
    int batchSize = getMessageBatchSize(topic);
    while (counter < batchSize) {
      //String msg = generateMessage();
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, generateMessage(topic, counter));
      producer.send(record);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      counter++;
    }
  }

  static String generateMessage(String topic, int counter) {
    String ser = null;
    if (DEFAULT_TOPIC.equals(topic)) {
      Action action = new Action((long) randomId.nextInt(10), ACTIONS[randomAction.nextInt(ACTIONS.length)]);
      ser = JSON.toJSONString(action);
    } else {
      Pattern pattern = patterns.get(counter);
      ser = JSON.toJSONString(pattern);
    }
    System.out.println(ser);
    return ser;
  }

  static int getMessageBatchSize(String topic) {
    int size = 0;
    if (DEFAULT_TOPIC.equals(topic)) {
      size = MESSAGE_BATCH_SIZE;
    } else { // pattern topic
      patterns = generateDefaultPatterns();
      size = patterns.size();
    }
    return size;
  }


  static List<Pattern> generateDefaultPatterns() {
    List<Pattern> patternList = new ArrayList<>();
    // Action的规则
    for (int i = 0, len = ACTIONS.length - 1; i < len; i++) {
      if (i < len - 1) {
        // 正常流程
        patternList.add(new Pattern(ACTIONS[i], ACTIONS[i + 1]));
        // 购买流程中断
        patternList.add(new Pattern(ACTIONS[i], ACTIONS[len]));
      }
    }
    return patternList;
  }


  public static void main(String[] args) {
    produceMessage(PATTERN_TOPIC);
    produceMessage(DEFAULT_TOPIC);
  }
}

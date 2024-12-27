package com.swj.sensors.flink_study.statebackend.statemachine;

import com.swj.sensors.flink_study.statebackend.statemachine.generataor.EventGeneratorSource;
import com.swj.sensors.flink_study.statebackend.statemachine.kafka.EventDeserializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/12 15:01
 */
public class KafkaEventGeneratorJob {

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    double errorRate = parameterTool.getDouble("error-rate", 0.0);
    int sleep = parameterTool.getInt("sleep", 1);
    String brokers = parameterTool.get("brokers", "localhost:9092");
    String topic = parameterTool.getRequired("kafka-topic");


    System.out.printf("Generating events to kafka with standalone source with error rate %f and sleep delay %d \n",
        errorRate, sleep);
    System.out.println();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.addSource(new EventGeneratorSource(errorRate, sleep))
        .addSink(new FlinkKafkaProducer011<>(brokers, topic, new EventDeserializer()));



    env.execute("state machine example Kafka event generator job");
  }
}

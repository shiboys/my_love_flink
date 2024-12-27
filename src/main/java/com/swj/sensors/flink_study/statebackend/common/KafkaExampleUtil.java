package com.swj.sensors.flink_study.statebackend.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/07 22:21
 */
public class KafkaExampleUtil {


  public static StreamExecutionEnvironment prepareExecutionEnv(ParameterTool parameterTool) throws Exception {
    if (parameterTool.getNumberOfParameters() < 4) {
      String message = "Missing Parameter!\n"
          + "Usage: --input-topic <topic> --output-topic <topic>"
          + " --bootstrap.servers:<kafka brokers> --group.id <groupId>";
      System.out.println(message);
      throw new Exception(message);
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(5_000); // 5s 一次 checkpoint
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 设置时间类型

    //make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(parameterTool);
    // 重启 4 次，每次 10 秒钟？
    env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10_000));

    return env;
  }
}

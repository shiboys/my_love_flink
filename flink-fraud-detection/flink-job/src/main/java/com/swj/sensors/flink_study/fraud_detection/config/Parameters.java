package com.swj.sensors.flink_study.fraud_detection.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/14 17:59
 * 对 Flink parameterTool 的封装，提供了更加简易的使用方式
 */
public class Parameters {

  private final ParameterTool parameterTool;

  public Parameters(ParameterTool tool) {
    this.parameterTool = tool;
  }


  public static Parameters fromArgs(String[] args) {
    ParameterTool tool = ParameterTool.fromArgs(args);
    return new Parameters(tool);
  }

  <T> T getOrDefault(Param<T> param) {
    if (!parameterTool.has(param.getName())) {
      return param.getDefaultValue();
    }
    Object value;

    if (param.getClazzType() == Integer.class) {
      value = parameterTool.getInt(param.getName());
    } else if (param.getClazzType() == Long.class) {
      value = parameterTool.getLong(param.getName());
    } else if (param.getClazzType() == Double.class) {
      value = parameterTool.getDouble(param.getName());
    } else if (param.getClazzType() == Boolean.class) {
      value = parameterTool.getBoolean(param.getName());
    } else {
      value = parameterTool.get(param.getName());
    }

    return param.getClazzType().cast(value);
  }


  // kafka:
  public static final Param<String> KAFKA_HOST = Param.string("kafka-host", "localhost");
  public static final Param<Integer> KAFKA_PORT = Param.integer("kafka-port", 9092);
  public static final Param<String> OFFSET = Param.string("offset", "latest");

  // 实时交易
  public static final Param<String> DATA_TOPIC = Param.string("data-topic", "livetransactions");
  public static final Param<String> ALERTS_TOPIC = Param.string("alerts-topic", "alerts");
  public static final Param<String> RULES_TOPIC = Param.string("rules-topic", "rules");
  public static final Param<String> LATENCY_TOPIC = Param.string("latency_topic", "latency");
  public static final Param<String> RULES_EXPORT_TOPIC = Param.string("current-rules-topic", "current-rules");

  // GCP PUBSUB google 的 消息队列系统

  public static final Param<String> GCP_PROJECT_NAME = Param.string("gcp-project", "da-fe-2023-2014");
  public static final Param<String> GCP_PUBSUB_RULES_SUBSCRIPTION = Param.string("pubsub-rules", "rules-demo");
  public static final Param<String> GCP_PUBSUB_ALERTS_SUBSCRIPTION = Param.string("pubsub-alerts", "alerts-demo");
  public static final Param<String> GCP_PUBSUB_LATENCY_SUBSCRIPTION = Param.string("pubsub-latency", "latency-demo");
  public static final Param<String> GCP_PUBSUB_RULES_EXPORT_SUBSCRIPTION =
      Param.string("pubsub-rules-export", "current-rules-demo");

  // socket
  public static final Param<Integer> SOCKET_PORT = Param.integer("socket-port", 9999);

  //general，source/sink types， kafka / pubsub / sockets

  public static final Param<String> RULES_SOURCE = Param.string("rules-source", "SOCKET");
  public static final Param<String> TRANSACTIONS_SOURCE = Param.string("data-source", "GENERATOR");
  public static final Param<String> ALERTS_SINK = Param.string("alerts-sink", "STDOUT");
  public static final Param<String> LATENCY_SINK = Param.string("latency-sink", "STDOUT");
  public static final Param<String> RULES_EXPORT_SINK = Param.string("rules-export-sink", "STDOUT");

  public static final Param<Integer> RECORDS_PER_SECOND = Param.integer("records-per-second", 2);

  public static final String LOCAL_MODE_DISABLE_WEB_UI = "-1";


  public static final Param<String> LOCAL_EXECUTION = Param.string("local", LOCAL_MODE_DISABLE_WEB_UI);

  public static final Param<Integer> SOURCE_PARALLELISM = Param.integer("source-parallelism", 2);
  public static final Param<Integer> SINK_PARALLELISM = Param.integer("sink-parallelism", 1);
  // 10 分钟
  public static final Param<Integer> CHECKPOINT_INTERVAL = Param.integer("checkpoint-interval", 60_000_0);
  public static final Param<Integer> MIN_PAUSE_BETWEEN_CHECKPOINTS =
      Param.integer("min-pause-between-checkpoints", 60_000_0);
  //乱序延迟窗口默认时长 500ms
  public static final Param<Integer> OUT_OF_ORDERNESS = Param.integer("out-of-orderness", 500);


  public static final List<Param<String>> STRING_PARAMS = Arrays.asList(
      LOCAL_EXECUTION,
      KAFKA_HOST,
      DATA_TOPIC,
      ALERTS_TOPIC,
      RULES_TOPIC,
      OFFSET,
      LATENCY_TOPIC,
      RULES_EXPORT_TOPIC,
      GCP_PROJECT_NAME,
      GCP_PUBSUB_RULES_SUBSCRIPTION,
      GCP_PUBSUB_ALERTS_SUBSCRIPTION,
      GCP_PUBSUB_LATENCY_SUBSCRIPTION,
      GCP_PUBSUB_RULES_EXPORT_SUBSCRIPTION,
      RULES_SOURCE,
      TRANSACTIONS_SOURCE,
      ALERTS_SINK,
      LATENCY_SINK,
      RULES_EXPORT_SINK
  );

  public static final List<Param<Integer>> INT_PARAMS = Arrays.asList(
      KAFKA_PORT,
      SOCKET_PORT,
      RECORDS_PER_SECOND,
      SOURCE_PARALLELISM,
      SINK_PARALLELISM,
      CHECKPOINT_INTERVAL,
      MIN_PAUSE_BETWEEN_CHECKPOINTS,
      OUT_OF_ORDERNESS
  );

  public static final List<Param<Boolean>> BOOLEAN_PARAMS = Collections.emptyList();
}

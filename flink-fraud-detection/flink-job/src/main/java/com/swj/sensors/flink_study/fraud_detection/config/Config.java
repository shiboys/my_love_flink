package com.swj.sensors.flink_study.fraud_detection.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/14 19:05
 */
public class Config {
  private final Map<Param<?>, Object> values = new HashMap<>();

  public <T> T put(Param<T> key, T value) {
    return (T) values.put(key, value);
  }

  public <T> T get(Param<T> key) {
    return key.getClazzType().cast(values.get(key));
  }

  public Config(Parameters inputParams,
      List<Param<String>> stringParams,
      List<Param<Integer>> intParams,
      List<Param<Boolean>> booleanParams) {
    overwriteDefaults(stringParams, inputParams);
    overwriteDefaults(intParams, inputParams);
    overwriteDefaults(booleanParams, inputParams);
  }

  public static Config fromParameters(Parameters inputParams) {
    return new Config(inputParams,
        Parameters.STRING_PARAMS,
        Parameters.INT_PARAMS,
        Parameters.BOOLEAN_PARAMS);
  }

  private <T> void overwriteDefaults(List<Param<T>> paramList, Parameters inputParams) {
    for (Param<T> param : paramList) {
      // 从输入参数中读取该变量的值
      put(param, inputParams.getOrDefault(param));
    }
  }
}

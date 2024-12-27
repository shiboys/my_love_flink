package com.swj.sensors.flink_study.fraud_detection.config;

import lombok.Getter;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/14 17:31
 */
@Getter
public class Param<T> {
  private String name;
  private Class<T> clazzType;
  private T defaultValue;

  public Param(String name, T defaultValue, Class<T> clazzType) {
    this.name = name;
    this.clazzType = clazzType;
    this.defaultValue = defaultValue;
  }

  public static Param<String> string(String name, String defaultValue) {
    return new Param<>(name, defaultValue, String.class);
  }

  public static Param<Integer> integer(String name, Integer defaultValue) {
    return new Param<>(name, defaultValue, Integer.class);
  }

  public static Param<Boolean> bool(String name, Boolean defaultValue) {
    return new Param<>(name, defaultValue, Boolean.class);
  }


}

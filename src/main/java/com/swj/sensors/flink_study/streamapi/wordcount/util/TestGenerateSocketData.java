package com.swj.sensors.flink_study.streamapi.wordcount.util;

import java.util.Random;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/29 22:00
 */
public class TestGenerateSocketData {
  public static void main(String[] args) {
    Random random = new Random();
    int bound = 50_000;
    int level = 26;
    long ts = System.currentTimeMillis();
    for (int i = 0; i < level; i++) {
      char ch = (char) ('A' + random.nextInt(level));
      long rts = ts + random.nextInt(bound);
      System.out.println(ch + "," + rts);
    }
  }
}

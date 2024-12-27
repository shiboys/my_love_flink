package com.swj.sensors.flink_study.streamapi.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/27 21:05
 */
public class WordCount {

  public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
      String[] splitArr = value.toLowerCase().split("\\W+");
      System.out.println("flatmap value is " + value);
      for (String token : splitArr) {
        if (token != null && !token.isEmpty()) {
          out.collect(new Tuple2<>(token, 1));
        }
      }
    }
  }


}

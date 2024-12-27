package com.swj.sensors.flink_study.streamapi.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 15:50
 */
public class StreamingSourceFromCollection {

  public static void main(String[] args) throws Exception {

    List<Integer> list = new ArrayList<>();
    list.addAll(Arrays.asList(1,2,3,4,5));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    SingleOutputStreamOperator<Integer> numStream = env.fromCollection(list).map(new MapFunction<Integer, Integer>() {
      @Override
      public Integer map(Integer value) throws Exception {
        return value + 1;
      }
    });

    numStream.print().setParallelism(1);
    env.execute(StreamingSourceFromCollection.class.getName());
  }
}

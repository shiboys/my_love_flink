package com.swj.sensors.flink_study.statebackend.keyedstate;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/06 14:41
 */
public class MapWithCounter {

  private static final String TOTAL_LENGTH_STATE_NAME = "total length of string";


  static class MapWithCounterFunction extends RichMapFunction<Tuple2<String, String>, Long> {

    ValueState<Long> totalLengthState;

    @Override
    public void open(Configuration parameters) throws Exception {
      ValueStateDescriptor<Long> descriptor =
          new ValueStateDescriptor<>(TOTAL_LENGTH_STATE_NAME, BasicTypeInfo.LONG_TYPE_INFO);
      totalLengthState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Long map(Tuple2<String, String> stringStringTuple2) throws Exception {
      Long currLength = totalLengthState.value();
      if (currLength == null) {
        currLength = 0L;
      }

      currLength += stringStringTuple2.f1.length();
      totalLengthState.update(currLength);
      return null;
    }
  }
}

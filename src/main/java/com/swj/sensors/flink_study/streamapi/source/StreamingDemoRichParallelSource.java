package com.swj.sensors.flink_study.streamapi.source;

import com.swj.sensors.flink_study.streamapi.source.custom.CustomRichParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 15:41
 */
public class StreamingDemoRichParallelSource {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<Long> textStream = env.addSource(new CustomRichParallelSource()).setParallelism(2);

    SingleOutputStreamOperator<Long> mapStream = textStream.map(new MapFunction<Long, Long>() {
      @Override
      public Long map(Long value) throws Exception {
        System.out.println("接收到数据：" + value);
        return value;
      }
    });

    SingleOutputStreamOperator<Long> sumStream = mapStream.timeWindowAll(Time.seconds(2)).sum(0);

    sumStream.print().setParallelism(1);

    env.execute(StreamingDemoRichParallelSource.class.getName());
    // 由于是 parallel 的，无法测试，这里就不测试了，但是 数据源的 accumulator 在什么地方使用了？？
  }
}

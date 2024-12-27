package com.swj.sensors.flink_study.streamapi.source;

import com.swj.sensors.flink_study.streamapi.source.custom.CustomParallelDataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 15:22
 */
public class StreamingDemoParallelDataSource {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 这里就可以设置多并行度
    DataStreamSource<Tuple2<String, Long>> textSource =
        env.addSource(new CustomParallelDataSource()).setParallelism(5);

    SingleOutputStreamOperator<Long> mapSource = textSource.map(new MapFunction<Tuple2<String, Long>, Long>() {
      @Override
      public Long map(Tuple2<String, Long> value) throws Exception {
        System.out.println("接收到数据：threadName：" + value.f0 + ", value= " + value.f1);
        return value.f1;
      }
    });

    // 每 2 秒处理一次数据
    SingleOutputStreamOperator<Long> sumStream = mapSource.timeWindowAll(Time.seconds(2)).sum(0);

    // 打印的地方设置并行度为 1 ，方便观察？？
    sumStream.print().setParallelism(1);

    env.execute(StreamingDemoParallelDataSource.class.getName());
  }
}

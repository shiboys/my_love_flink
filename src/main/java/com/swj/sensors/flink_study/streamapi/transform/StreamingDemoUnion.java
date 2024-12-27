package com.swj.sensors.flink_study.streamapi.transform;

import com.swj.sensors.flink_study.streamapi.source.custom.CustomNoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 16:50
 * union
 * 合并多个流，新的流会包含所有流中的数据，但是 union 有个限制，就是合并的流类型必须一致
 */
public class StreamingDemoUnion {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<Long> stream1 = env.addSource(new CustomNoParallelSource()).setParallelism(1);

    DataStreamSource<Long> stream2 = env.addSource(new CustomNoParallelSource()).setParallelism(1);

    DataStream<Long> unionStream = stream1.union(stream2);

    SingleOutputStreamOperator<Long> mapStream = unionStream.map(new MapFunction<Long, Long>() {
      @Override
      public Long map(Long value) throws Exception {
        System.out.println("原始收到的数据：" + value);
        return value;
      }
    });

    // 2 秒内的窗口元素进行求和计算
    SingleOutputStreamOperator<Long> sumStream = mapStream.timeWindowAll(Time.seconds(2)).sum(0);

    // 打印求和计算的结果
    sumStream.print().setParallelism(1);

    env.execute(StreamingDemoUnion.class.getName());
    /**
     * 打印结果就是这样的：
     * 原始收到的数据：32
     * 原始收到的数据：32
     * 原始收到的数据：33
     * 原始收到的数据：33
     * 130
     * 原始收到的数据：34
     * 原始收到的数据：34
     * 原始收到的数据：35
     * 原始收到的数据：35
     * 138
     */
  }
}

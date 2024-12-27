package com.swj.sensors.flink_study.streamapi.partitioner;

import com.swj.sensors.flink_study.streamapi.source.custom.CustomNoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 18:32
 */
public class StreamingWithCustomPartitioner {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 为了测试自定义分区功能，自定义分区分了 2 个分区，这里并行度设为 2
    env.setParallelism(2);
    DataStreamSource<Long> dataStream = env.addSource(new CustomNoParallelSource());

    // 对数据进行转化， 将 Long 类型 转换成 Tuple1 类型
    SingleOutputStreamOperator<Tuple1<Long>> mappedStream = dataStream.map(new MapFunction<Long, Tuple1<Long>>() {
      @Override
      public Tuple1<Long> map(Long value) throws Exception {
        return new Tuple1<>(value);
      }
    });

    // 用 第 1 个字段来作为 自定义分区器的 key
    DataStream<Tuple1<Long>> partitionedStream = mappedStream.partitionCustom(new CustomPartitioner(), 0);


    SingleOutputStreamOperator<Long> finalStream = partitionedStream.map(new MapFunction<Tuple1<Long>, Long>() {
      @Override
      public Long map(Tuple1<Long> value) throws Exception {
        // 打印当然前程，查看分区效果
        System.out.println("当前线程 Id：" + Thread.currentThread().getId() + ", value: " + value);
        return value.getField(0);
      }
    });

    finalStream.print().setParallelism(1);

    env.execute(StreamingWithCustomPartitioner.class.getName());
  }
}

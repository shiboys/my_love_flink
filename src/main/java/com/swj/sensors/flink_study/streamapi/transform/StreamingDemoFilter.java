package com.swj.sensors.flink_study.streamapi.transform;

import com.swj.sensors.flink_study.streamapi.source.custom.CustomNoParallelSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 17:02
 * 根据规则，把一个数据流分成多个流
 * 应用场景：
 * 在实际的工作场景中，源数据流可能混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以根据一定的规则
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不同的处理逻辑了。
 */
public class StreamingDemoFilter {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<Long> dataStream = env.addSource(new CustomNoParallelSource()).setParallelism(1);

    // 对数据流进行过滤, filter 返回true 的数据会被留下
    SingleOutputStreamOperator<Long> filterStream = dataStream
        .map(new MapFunction<Long, Long>() {
          @Override
          public Long map(Long value) throws Exception {
            System.out.println("接收到的原始数据：" + value);
            return value;
          }
        })
        /**
         * 设置数据流分区，以便元素的输出能够被广播(复制)到下游的每一个算子
         * Sets the partitioning of the {@link DataStream} so that the output elements
         * are broadcasted to every parallel instance of the next operation.
         *
         * broadcast() 算子的含义
         */
        //.broadcast()
        .filter(new FilterFunction<Long>() {
          @Override
          public boolean filter(Long value) throws Exception {
            // 把所有奇数给过滤掉
            return value % 2 == 0;
          }
        });



    SingleOutputStreamOperator<Long> sumStream = filterStream.map(new MapFunction<Long, Long>() {
      @Override
      public Long map(Long value) throws Exception {
        System.out.println("过滤后的数据：" + value);
        return value;
      }
    })
        .timeWindowAll(Time.seconds(2))
        .sum(0);

    sumStream.print().setParallelism(1);

    env.execute(StreamingDemoFilter.class.getName());
  }
}

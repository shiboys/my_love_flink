package com.swj.sensors.flink_study.streamapi.source;

import com.swj.sensors.flink_study.streamapi.source.custom.CustomNoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 14:39
 */
public class StreamingDemoWithoutParallelSource {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 应为 CustomNoParallelSource 不是并行的 DataSource ,所以这里必须把算子的并行度设为 1
    // 防止外部配置文件或者默认并行度将此改为 > 1 的 数，因为 dataSource 不支持
    DataStreamSource<Long> textStream = env.addSource(new CustomNoParallelSource()).setParallelism(1);
    SingleOutputStreamOperator<Long> mapSource = textStream.map(new MapFunction<Long, Long>() {
      @Override
      public Long map(Long value) throws Exception {
        System.out.println("接收到数据：" + value);
        return value;
      }
    });

    // 求 2 秒内数字的和。窗口为滚动窗口
    SingleOutputStreamOperator<Long> sumStream = mapSource.timeWindowAll(Time.seconds(2)).sum(0);
    // 10 秒窗口内的 最大数字
    SingleOutputStreamOperator<Long> maxStream = mapSource.timeWindowAll(Time.seconds(10)).max(0);

    // 求和的输出 1=2=3,3+4= 7,5+6=11...
    sumStream.print().setParallelism(1);
    maxStream.print().setParallelism(1);

    env.execute(StreamingDemoWithoutParallelSource.class.getName());
    /**
     * 打印结果如下：
     * 接收到数据：82
     * 接收到数据：83
     * 165
     * 接收到数据：84
     * 接收到数据：85
     * 169
     * 接收到数据：86
     * 接收到数据：87
     * 173
     * 接收到数据：88
     * 接收到数据：89
     * 177
     * 接收到数据：90
     * 接收到数据：91
     * 181
     * 91
     * 2 秒的窗口计算一次和。10 秒的窗口计算一次最大数。
     */
  }
}

package com.swj.sensors.flink_study.streamapi.transform;

import com.swj.sensors.flink_study.streamapi.source.custom.CustomNoParallelSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 17:02
 *
 * 根据规则，把一个数据流分成多个流
 *
 * 应用场景：
 * 在实际的工作场景中，源数据流可能混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以根据一定的规则
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不同的处理逻辑了。
 */
public class StreamingDemoSplit {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<Long> dataStream = env.addSource(new CustomNoParallelSource()).setParallelism(1);

    // 对数据流进行切分，按 Long 的奇偶数性进行
    SplitStream<Long> splitStream = dataStream.split(new OutputSelector<Long>() {
      List<String> list = new ArrayList<>();

      @Override
      public Iterable<String> select(Long value) {
        if (value % 2 == 0) {
          list.add("even");
        } else {
          list.add("odd");
        }
        return list;
      }
    });

    // 选择一个或多个切分后的数据流
    DataStream<Long> evenStream = splitStream.select("event");
    DataStream<Long> oddStream = splitStream.select("odd");

    DataStream<Long> allStream = splitStream.select("even","odd");

    allStream.print().setParallelism(1);

    env.execute(StreamingDemoSplit.class.getName());
  }
}

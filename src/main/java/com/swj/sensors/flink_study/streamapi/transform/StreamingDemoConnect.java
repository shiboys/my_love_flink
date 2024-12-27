package com.swj.sensors.flink_study.streamapi.transform;

import com.swj.sensors.flink_study.streamapi.source.custom.CustomNoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 15:58
 * connect 与 union 不同，但是只能连接两个流，两个流中的数据类型可以不同，
 * 会对两个不同的流中的数据应用不同的处理方法。
 */
public class StreamingDemoConnect {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<Long> longSource = env.addSource(new CustomNoParallelSource()).setParallelism(1);

    SingleOutputStreamOperator<String> textSource = env.addSource(new CustomNoParallelSource()).setParallelism(1).map(
        new MapFunction<Long, String>() {
          @Override
          public String map(Long value) throws Exception {
            return "str_" + value;
          }
        });

    // 分别处理，两个流中的数据类型可以不同
    SingleOutputStreamOperator<Object> connectedSource =
        longSource.connect(textSource).map(new CoMapFunction<Long, String, Object>() {
          @Override
          public Object map1(Long value) throws Exception {
            return value;
          }

          @Override
          public Object map2(String value) throws Exception {
            return value;
          }
        });

    connectedSource.print().setParallelism(1);

    env.execute(StreamingDemoConnect.class.getName());

  }

}

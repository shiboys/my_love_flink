package com.swj.sensors.flink_study.streamapi.types;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 17:31
 */
public class PojoClass {
  public static void main(String[] args) throws Exception {

    // 最基础的 typeInfo
    TypeInformation<Long> longInfo = BasicTypeInfo.LONG_TYPE_INFO;
    // 常用的 typeInfo
    TypeInformation<String> info = TypeInformation.of(String.class);
    // 需要使用 TypeHint 类型提示的 TypeInfomation
    TypeInformation<Tuple2<String, Long>> infoWithHint = TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
    });


    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<Integer> inputStream = env.fromElements(1, 2, 3);

    DataStream<Tuple2<Integer, Long>> result =
        inputStream.map(new AppendOne<>()).returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>() {
        }));

    result.print();

    env.execute(PojoClass.class.getName());

  }

  /**
   * AppendOne 就可以使用泛型，返回一个 Tuple2 封装类类型
   *
   * @param <T>
   */
  static class AppendOne<T> implements MapFunction<T, Tuple2<T, Long>> {
    @Override
    public Tuple2<T, Long> map(T value) throws Exception {
      return new Tuple2<>(value, 1L);
    }
  }
}

package com.swj.sensors.flink_study.streamapi.join;

import com.swj.sensors.flink_study.streamapi.utils.ThrottledIterator;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/29 14:46
 */
public class WindowJoinSampleData {

  static final String[] NAMES = {"tom", "jack", "peter", "lucy", "john", "grace"};
  static final int GRADE_COUNT = 5;
  static final int SALARY_MAX = 10000;


  public static class GradeSource implements Iterator<Tuple2<String, Integer>>, Serializable {

    final Random random = new Random(hashCode());

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public Tuple2<String, Integer> next() {
      return new Tuple2<>(NAMES[random.nextInt(NAMES.length)], random.nextInt(GRADE_COUNT) + 1);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    /**
     * 返回一个 基于 GradeSource 成成的 DataStream<Tuple2<String,Integer>
     *
     * @param env
     * @param generateRate
     * @return
     */
    public static DataStream<Tuple2<String, Integer>> dataSource(StreamExecutionEnvironment env, int generateRate) {
      // 这里用 fromCollection，没有用 addSource
      // 迭代器 GradeSource 的外面给它套上 限流器 ThrottledIterator。
      // 套马的汉子你威武雄壮。。。。
      return env.fromCollection(new ThrottledIterator<>(new GradeSource(), generateRate),
          TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
          }));
    }
  }


  public static class SalarySource implements Iterator<Tuple2<String, Integer>>, Serializable {

    final Random random = new Random(hashCode());

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public Tuple2<String, Integer> next() {
      return new Tuple2<>(NAMES[random.nextInt(NAMES.length)], random.nextInt(SALARY_MAX) + 1);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    public static DataStream<Tuple2<String, Integer>> dataSource(StreamExecutionEnvironment env, int generateRate) {
      return env.fromCollection(new ThrottledIterator<>(new SalarySource(), generateRate),
          TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
          }));
    }
  }
}

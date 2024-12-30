package com.swj.sensors.flink_study.table_api_with_sql.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/25 21:26
 */
public class SqlDemo {



  /**
   * Use first field for bucket assigner
   */
  public static class KeyBucketAssigner implements BucketAssigner<Row, String> {

    private static final long serialVersionUID = 987325769970523326L;

    @Override
    public String getBucketId(Row element, Context context) {
      return String.valueOf(element.getField(0));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
      return SimpleVersionedStringSerializer.INSTANCE;
    }
  }


  /**
   * Kills the first execution attempt of an application when it receives the second record
   * 接收到第二条消息，则中断当前算子
   */
  public static class KillMapper implements MapFunction<Row, Row>, ListCheckpointed<Integer> {

    // count all processed records of all previous execution attempts;
    private int savedRecordsCount;

    // counted this processed records of this execution record.
    private int currentRecordsCount;


    @Override
    public Row map(Row value) throws Exception {
      if (savedRecordsCount == 10 && currentRecordsCount == 10) {
        // 之所以这么做，是为了抛弃第一条第二条记录？
        /**
         * 运行结果如下：
         * 0,2023-12-25 14:34:12.231,1
         * 2,2023-12-25 14:34:12.235,1
         * 0,2023-12-25 14:34:23.34,1
         * 1,2023-12-25 14:34:23.34,1
         * 0,2023-12-25 14:34:34.381,1
         * 2,2023-12-25 14:34:34.381,1
         * 0,2023-12-25 14:34:45.471,1
         * 2,2023-12-25 14:34:45.471,1
         * 10 秒的窗口，就是说只运行前两条的记录，第二条记录的时候 抛出异常
         */
        // savedRecordsCount ！= currentRecordsCount 是发生在 从 checkpoint 重启之后
        // 这里抛出异常，就是为了让 flink 自动从 checkpoint 重启恢复状态, 从而导致这两个计数器的值不一样。
        throw new RuntimeException("Killed this Job!");
      }

      savedRecordsCount++;

      currentRecordsCount++;

      return value;
    }

    @Override
    public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
      return Collections.singletonList(savedRecordsCount);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
      int ls = 0;
      for (Integer val : state) {
        ls += val;
      }
      System.out.println("savedRecordsCount value restored from state is " + ls);
      savedRecordsCount += ls;
    }
  }
}

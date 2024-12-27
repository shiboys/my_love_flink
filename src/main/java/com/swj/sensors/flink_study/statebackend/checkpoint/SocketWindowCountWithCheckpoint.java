package com.swj.sensors.flink_study.statebackend.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/06 17:13
 * checkpoint 的笔记可以参考 flink 官网的这篇笔记 https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/state/checkpoints/
 */
public class SocketWindowCountWithCheckpoint {
  public static void main(String[] args) throws Exception {
    int port;
    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    try {
      port = parameterTool.getInt("port");
    } catch (Exception e) {
      port = 9000;
      System.out.println("no port set!! use default port 9000");
    }
    String host = parameterTool.get("host", "localhost");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     *  Checkpoint 的生命周期 由 Flink 管理， 即 Flink 创建，管理和删除 checkpoint - 无需用户交互。
     *  由于 checkpoint 被经常触发，且被用于作业恢复，所以 Checkpoint 的实现有两个设计目标：i）轻量级创建和 ii）尽可能快地恢复。
     *
     *  在用户终止作业后，会自动删除 Checkpoint（除非明确配置为保留的 Checkpoint）。
     * Checkpoint 以状态后端特定的（原生的）数据格式存储（有些状态后端可能是增量的）。
     *
     * 尽管 savepoints 在内部使用与 checkpoints 相同的机制创建，但它们在概念上有所不同，并且生成和恢复的成本可能会更高一些。
     * Savepoints的设计更侧重于可移植性和操作灵活性，尤其是在 job 变更方面。Savepoint 的用例是针对计划中的、手动的运维。
     * 例如，可能是更新你的 Flink 版本，更改你的作业图等等。
     */
    // 这里 开启 checkpoint，经过查官方文档，发现 checkpoint 的开启只能通过代码开启
    // 每 1 秒开启一个 检查点
    env.enableCheckpointing(1000);

    // 高级选项
    // 设置模式为 Exactly-Once ，这个是默认值
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    // checkpoint 超时时间 1 分钟
    env.getCheckpointConfig().setCheckpointTimeout(60_000);
    // checkpoint 的并发度为 1，也就是同一时间只允许进行一个检查点。应该是 这个 当前 checkpoint 没有完成，下一个 checkpoint jobmananger 不能生成
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    //表示一旦 flink job 被 cancel 后，会保留 checkpoint 数据，以便根据实际需要恢复到指定的 checkpoint
    // DELETE_ON_CANCELLATION 表示 flink job 一旦被 cancel 后，会删除 checkpoint 数据， 只有 job 执行失败时才会保存 checkpoint ?
    // RETAIN_ON_CANCELLATION 表示 flink job 一旦被 cancel 后，会保留checkpoint，以便根据需要恢复到指定的 checkpoint
    env.getCheckpointConfig().enableExternalizedCheckpoints(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    // 设置 statebackend
    // FileSystemCheckpointStorage 配置中包含文件系统 URL（类型、地址、路径）， 例如 “hdfs://namenode:40010/flink/checkpoints”
    // 或 “file:///data/flink/checkpoints”。
    env.setStateBackend(new FsStateBackend(
        "file://" + System.getProperty("user.dir") + "Volumes/home/github/my_love_flink/checkpoints"));

    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);


    DataStreamSource<String> socketTextStream = env.socketTextStream(host, port, "\n");

    SingleOutputStreamOperator<WordCount> wordCountStream =
        socketTextStream.flatMap((FlatMapFunction<String, WordCount>) (s, collector) -> {
          String[] words = s.split("\\s");
          for (String word : words) {
            collector.collect(new WordCount(word, 1));
          }
        })
            .keyBy("word")
            .timeWindow(Time.seconds(2), Time.seconds(1)) // 指定窗口为 2 秒，指定时间间隔为 1 秒
            .sum("count");// 也可以使用 reduce function
//        .reduce(new ReduceFunction<WordCount>() {
//          @Override
//          public WordCount reduce(WordCount wordCount, WordCount t1) throws Exception {
//
//            return new WordCount(wordCount.word, wordCount.count + t1.count);
//          }
//        })

    wordCountStream.print().setParallelism(1);
    env.execute("socket word count with checkpoint");
    /**
     * 增量快照 #
     * RocksDB 支持增量快照。不同于产生一个包含所有数据的全量备份，增量快照中只包含自上一次快照完成之后被修改的记录，因此可以显著减少快照完成的耗时。
     *
     * 一个增量快照是基于（通常多个）前序快照构建的。由于 RocksDB 内部存在 compaction 机制对 sst 文件进行合并，
     *  Flink 的增量快照也会定期重新设立起点（rebase），因此增量链条不会一直增长，旧快照包含的文件也会逐渐过期并被自动清理。
     */
  }


  static class WordCount {
    public String word;
    public int count;

    public WordCount(String word, int count) {
      this.word = word;
      this.count = count;
    }

    @Override
    public String toString() {
      return "WordCount{" +
          "word='" + word + '\'' +
          ", count=" + count +
          '}';
    }
  }
}

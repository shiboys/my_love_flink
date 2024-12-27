package com.swj.sensors.flink_study.streamapi.source.custom;

import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 15:35
 */
public class CustomRichParallelSource extends RichParallelSourceFunction<Long> {
  volatile boolean isRunning = true;

  private long counter;


  @Override
  public void run(SourceContext<Long> ctx) throws Exception {
    while (isRunning) {
      ctx.collect(++counter);
      getRuntimeContext().getAccumulator("accumulator").add(counter);
      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }


  /**
   * 只在启动过程中调用一次，实现对 Function 状态中的初始化
   *
   * @param parameters
   * @throws Exception
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    System.out.println("Executing the open method: ThreadName:" + Thread.currentThread().getName());
    getRuntimeContext().addAccumulator("accumulator", new AverageAccumulator());
    super.open(parameters);
  }

  /**
   * 实现关闭连接的代码
   *
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    super.close();
  }
}

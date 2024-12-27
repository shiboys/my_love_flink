package com.swj.sensors.flink_study.statebackend.statemachine.generataor;

import com.swj.sensors.flink_study.statebackend.statemachine.event.Event;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/11 20:11
 * 该类的主要作用是用状态机来模拟生成事件，并 push 到诸如 kafka 之中
 */
public class StandaloneThreadedGenerator {

  public static void runCollector(Collector<Event>[] collectors) throws IOException {
    GeneratorThread[] threads = new GeneratorThread[collectors.length];
    int maxValue = Integer.MAX_VALUE;
    int step = maxValue / collectors.length;
    int minIp = 0, maxIp = 0;
    for (int i = 0; i < threads.length; i++) {
      minIp = step * i;
      maxIp = minIp + step;
      threads[i] = new GeneratorThread(collectors[i], minIp, maxIp);
      threads[i].setName("Generator " + i);
    }

    long delay = 2;
    int nextErroneousIdx = 0;
    boolean isRunning = true;
    // 启动所有子线程，包括统计线程
    for (GeneratorThread thread : threads) {
      thread.setDelay(delay);
      thread.start();
    }
    ThroughputLogger loggerThread = new ThroughputLogger(threads);
    loggerThread.start();

    // 主线程打印用法
    System.out.println("Usage:");
    System.out.println(" -> q : Quit");
    System.out.println(" -> + : increase latency");
    System.out.println(" -> - : decrease latency");
    System.out.println(" -> e : inject invalid state transition");

    while (isRunning) {
      int op = System.in.read();
      switch (op) {
        case 'q':
          isRunning = false;
          break;
        case '+':
          delay = Math.max(delay * 2, 1);
          System.out.println("delay is " + delay);
          for (GeneratorThread thread : threads) {
            thread.setDelay(delay);
          }
          break;
        case '-':
          delay /= 2;
          System.out.println("delay is " + delay);
          for (GeneratorThread thread : threads) {
            thread.setDelay(delay);
          }
          break;
        case 'i':
          // 找到指定的线程，通知该线程，让该线程的搜集器需要产生一个异常事件
          threads[nextErroneousIdx].setInvalidStateTransition();
          nextErroneousIdx = (nextErroneousIdx + 1) % threads.length;
          break;
      }
    }
    // user input the quit command
    // shutdown
    loggerThread.shutdown();
    for(GeneratorThread thread: threads) {
      thread.shutdown();
      // 子线程可能还没有结束运行，主线程需要等待
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }


  // 运行 EventGenerator 的定制线程
  private static class GeneratorThread extends Thread {
    private final int minIp;
    private final int maxIp;
    private final Collector<Event> collector;

    private volatile boolean isRunning;
    private volatile boolean injectInvalidNext;
    private long delay;
    private int count;

    public GeneratorThread(Collector<Event> collector, int minIp, int maxIp) {
      this.maxIp = maxIp;
      this.minIp = minIp;
      this.collector = collector;
      isRunning = true;
    }

    @Override
    public void run() {
      final EventGenerator generator = new EventGenerator();

      while (isRunning) {
        if (injectInvalidNext) { // 发送 异常事件的开关打开
          injectInvalidNext = false; // 异常开关使用一次后就关闭，每次开启必须手动打开
          collector.collect(generator.nextInvalid());
        } else { // 发送正常的数据
          collector.collect(generator.next(minIp, maxIp));
        }
        count += 1;
        if (delay > 0) {
          try {
            Thread.sleep(delay);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }

    }

    public void setDelay(long delay) {
      this.delay = delay;
    }

    public int getCount() {
      return count;
    }

    public void setInvalidStateTransition() {
      this.injectInvalidNext = true;
    }

    public void shutdown() {
      isRunning = false;
      interrupt();
    }

  }


  /**
   * 该线程的主要作用是收集 所有的 GeneratorThread 的事件发送量
   * 并且每秒统计一次，然后将统计结果打印出来
   */
  private static class ThroughputLogger extends Thread {
    private volatile boolean isRunning;
    private final GeneratorThread[] generators;

    public ThroughputLogger(GeneratorThread[] generators) {
      this.generators = generators;
      this.isRunning = true;
    }

    @Override
    public void run() {
      long lastCalcTime = System.currentTimeMillis();
      long lastCount = 0;
      while (isRunning) {
        try {
          Thread.sleep(1000);
          long newCount = 0;
          for (GeneratorThread generatorThread : generators) {
            newCount += generatorThread.getCount();
          }
          long now = System.currentTimeMillis();
          double timeElapsedSeconds = (now - lastCalcTime) / 1000.0;
          double collectSpeedPerSec = (newCount - lastCount) / timeElapsedSeconds;

          lastCalcTime = now;
          lastCount = newCount;
          System.out.println(collectSpeedPerSec + "/ sec");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    public void shutdown() {
      isRunning = false;
      interrupt();
    }
  }
}

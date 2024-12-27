package com.swj.sensors.flink_study.streamapi.utils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/29 12:04
 * 迭代器 Iterator 的简单限流
 * 进入 Flink 的数据源必须实现 Serializable
 */
public class ThrottledIterator<T> implements Iterator<T>,Serializable {

  private static final long serialVersionUID = 1L;

  // 每迭代 batchSize 个元素，则 sleep batch time 毫秒
  private int speedBatchSize;
  private int speedBatchTime;
  // 发送数统计
  private int num;

  private Iterator<T> source;

  /**
   * 上次批发送产生的时间
   */
  private long lastBatchCheckTime;

  /**
   * @param sourceIterator 迭代器
   * @param generateRate   限制每秒迭代器产生多少个元素
   */
  public ThrottledIterator(Iterator<T> sourceIterator, int generateRate) {
    Objects.requireNonNull(sourceIterator);
    if (!(sourceIterator instanceof Serializable)) {
      // 强调 sourceIterator 必须实现 Serializable 接口的原因是 flink 可能会将 sourceIterator 进行状态持久化
      throw new IllegalArgumentException("source iterator is not instance of java.io.Serializable");
    }
    this.source = sourceIterator;
    if (generateRate >= 100) {
      this.speedBatchTime = 50;
      // 每 50 ms 产生多少个
      this.speedBatchSize = generateRate / 20;
    } else if (generateRate >= 1) {
      this.speedBatchSize = 1;
      // 每生成一个元素需要多少 ms
      this.speedBatchTime = 1000 / generateRate;
    } else {
      throw new IllegalArgumentException("generateRate must be positive and greater than zero.");
    }
  }

  @Override
  public boolean hasNext() {
    return source.hasNext();
  }

  @Override
  public T next() {
    // 计算延迟情况
    // delay if necessary
    if (lastBatchCheckTime > 0) {
      if (++num >= this.speedBatchSize) {
        num = 0;// 重置
        long now = System.currentTimeMillis();
        long timeElapsed = now - lastBatchCheckTime;
        long timeDiff = this.speedBatchTime - timeElapsed;
        if (timeDiff > 0) {
          try {
            Thread.sleep(timeDiff);
          } catch (InterruptedException e) {
            // 恢复 interrupted 标识
            Thread.currentThread().interrupt();
          }
        }
        // 重置 上次批发送的为 now
        lastBatchCheckTime = now;
      }
    } else {
      lastBatchCheckTime = System.currentTimeMillis();
    }
    return source.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}

package com.swj.sensors.flink_study.streamapi.partitioner;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/11/30 18:28
 */
public class CustomPartitioner implements Partitioner<Long> {
  @Override
  public int partition(Long key, int numPartitions) {
    System.out.println("分区总数为：" + numPartitions);
    if (key % 2 == 0) {
      return 0;
    } else {
      return 1;
    }
  }
}

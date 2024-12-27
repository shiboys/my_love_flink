package com.swj.sensors.flink_study.statebackend.broadcaststate;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/07 22:28
 */
public class SimpleBroadcastProcessExample {

  /**
   * 广播状态(Broadcast State)
   *
   * 广播状态是一种特殊的算子状态(Operator State)。引入它的目的是在于支持一个流中的元素需要广播到所有下游任务的适用情形。
   * 在这些任务重广播状态用于保持所有子任务状态相同？？
   * 该状态接下来可在第二个处理机的数据流中访问。可以设想包含了一些列初始化其他流中的元素规则的低吞吐量数据流，这个例子自然地运用了广播状态。
   * 考虑到上述使用情形，广播状态和其他算子状态的不同之处在于：
   * 1、它具有 map 格式；
   * 2、它仅在一些特殊的算子中可用。这些算子的输入被称为一个广播数据流和非广播数据流，
   * 3、这类算子可用拥有不同命名的多个广播状态。
   */

}

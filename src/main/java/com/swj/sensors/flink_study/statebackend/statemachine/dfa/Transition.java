package com.swj.sensors.flink_study.statebackend.statemachine.dfa;

import com.swj.sensors.flink_study.statebackend.statemachine.event.EventType;

import java.io.Serializable;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/11 11:59
 * 这个类给枚举用了， 所以需要 serializable 化
 */
public class Transition implements Serializable {

  //this class is serializable to be able to interact with enum
  private static final long serialVersionUID = 1L;

  // 触发的事件类型
  private final EventType eventType;

  // 事件触发后进入的目标状态
  private final State targetState;

  // 从事件类型到目标状态的发生几率
  private final float prob;


  public Transition(EventType eventType, State targetState, float prob) {
    this.eventType = eventType;
    this.targetState = targetState;
    this.prob = prob;
  }

  public EventType eventType() {
    return eventType;
  }

  public float prob() {
    return prob;
  }

  public State targetState() {
    return targetState;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Transition that = (Transition) o;
    return Float.compare(that.prob, prob) == 0 &&
        eventType == that.eventType &&
        targetState == that.targetState;
  }

  @Override
  public int hashCode() {
    int code = 31 * eventType.hashCode() + targetState.hashCode();

    return 31 * code + (prob != +0.0f ? Float.floatToIntBits(prob) : 0);
  }

  @Override
  public String toString() {
    return "Transition{" +
        "eventType=" + eventType +
        ", targetState=" + targetState +
        ", prob=" + prob +
        '}';
  }
}

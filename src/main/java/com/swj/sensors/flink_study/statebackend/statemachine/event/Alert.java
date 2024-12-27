package com.swj.sensors.flink_study.statebackend.statemachine.event;

import com.swj.sensors.flink_study.statebackend.statemachine.dfa.State;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/11 10:48
 * 错误状态的警告信息。toString 的时候，格式化为一条警告信息
 * 内容是从什么地方发出了一条错误的事件转化(transition) 导致当前的状态为 state。
 *
 */
public class Alert {

  private final int address;

  private final State state;

  private final EventType transition;

  public Alert(int address, State state, EventType eventType) {
    this.address = address;
    this.state = state;
    this.transition = eventType;
  }

  public int address() {
    return address;
  }

  public State state() {
    return state;
  }

  public EventType transition() {
    return transition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Alert alert = (Alert) o;
    return address == alert.address &&
        state == alert.state &&
        transition == alert.transition;
  }

  @Override
  public int hashCode() {
    int code = 31*address + state.hashCode();
    return 31*code + transition.hashCode();
  }

  @Override
  public String toString() {
    return "ALERT "
        + Event.formatAddress(address)
        + ": " + state.name()
        + "->"
        + transition.name();
  }
}

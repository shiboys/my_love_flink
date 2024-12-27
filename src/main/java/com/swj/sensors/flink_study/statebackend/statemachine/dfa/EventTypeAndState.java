package com.swj.sensors.flink_study.statebackend.statemachine.dfa;

import com.swj.sensors.flink_study.statebackend.statemachine.event.EventType;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/11 12:53
 */
public class EventTypeAndState{

  public final EventType eventType;

  public final State state;

  public EventTypeAndState(EventType eventType, State state) {
    this.eventType = eventType;
    this.state = state;
  }
}

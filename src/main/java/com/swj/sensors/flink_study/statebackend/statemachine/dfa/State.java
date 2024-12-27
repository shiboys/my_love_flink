package com.swj.sensors.flink_study.statebackend.statemachine.dfa;

import com.swj.sensors.flink_study.statebackend.statemachine.event.EventType;

import java.util.Random;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/11 11:50
 * 状态枚举，是整个 Flink 状态机的的核心定义类。状态的流转图如下所示：
 *           +--[a]--> W --[b]--> Y --[e]---+
 *           |                    ^         |
 *   Initial-+                    |         |
 *           |                    |         +--> (Z)-----[g]---> Terminal
 *           +--[c]--> X --[b]----+         |
 *                     |                    |
 *                     +--------[d]---------+
 */
public enum State {

  // 状态枚举，这里我们根据状态流转图，倒着枚举状态
  Terminal,
  InvalidTransition,
  Z(new Transition(EventType.g, Terminal, 1.0f)),
  Y(new Transition(EventType.e, Z, 1.0f)),
  X(new Transition(EventType.d, Z, 0.8f), new Transition(EventType.b, Y, 0.2f)),
  W(new Transition(EventType.b, Y, 1.0f)),
  Initial(new Transition(EventType.a, W, 0.6f), new Transition(EventType.c, X, 0.4f));

  private final Transition[] transitions;

  State(Transition... transitions) {
    this.transitions = transitions;
  }

  /**
   * 从当前状态根据传入的事件类型转换到当前状态的下一个状态。
   * 如果根据传入的事件类型不能转到下一个状态，则返回 Invalid
   *
   * @param eventType 事件类型
   * @return 目标状态
   */
  public State transition(EventType eventType) {
    for (Transition transition : transitions) {
      if (eventType == transition.eventType()) {
        return transition.targetState();
      }
    }
    //no illegal transition found
    return InvalidTransition;
  }

  public boolean isTerminal() {
    return transitions == null || transitions.length == 0;
  }

  /**
   * 通过一个随机算法，获取当前状态的下一个随机状态
   *
   * @param rdm
   * @return
   */
  public EventTypeAndState randomTransition(Random rdm) {
    if (isTerminal()) {
      throw new RuntimeException("can not transition from state " + name());
    } else {
      float prob = rdm.nextFloat();
      float pass = 0.0f;
      Transition ts = null;
      for (Transition transition : transitions) {
        pass += transition.prob();
        if (pass >= prob) {
          ts = transition;
          break;
        }
      }
      assert ts != null;
      return new EventTypeAndState(ts.eventType(), ts.targetState());
    }
  }


  /**
   * 当前状态 转化成 InvalidTransition 的状态，所有可能的 eventType 中，随机选择一个.
   * 该操作的目的是产生一个非法的事件(类型)
   *
   * @param random
   * @return
   */
  public EventType randomInvalidTransition(Random random) {
    while (true) {
      EventType candidate = EventType.values()[random.nextInt(EventType.values().length)];
      if (transition(candidate) == InvalidTransition) {
        return candidate;
      }
    }
  }
}

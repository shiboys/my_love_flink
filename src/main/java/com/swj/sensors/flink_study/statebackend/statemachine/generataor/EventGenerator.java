package com.swj.sensors.flink_study.statebackend.statemachine.generataor;

import com.swj.sensors.flink_study.statebackend.statemachine.dfa.EventTypeAndState;
import com.swj.sensors.flink_study.statebackend.statemachine.dfa.State;
import com.swj.sensors.flink_study.statebackend.statemachine.event.Event;
import com.swj.sensors.flink_study.statebackend.statemachine.event.EventType;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/11 15:26
 * EventGenerator 要实现 Serializable ，要不然 flink 提交保持，坑爹了
 */
public class EventGenerator implements java.io.Serializable {
  private static final long serialVersionUID = 11111L;
  /**
   * EventGenerator 的功能是提供 API ，产生正常和异常的事件，供调用方使用。
   * nextEvent 方法用于产生下一个正常的事件
   * nextInvalid 方法用于产生一个随机的异常事件
   */
  private final Random random;
  // 将所有的IP 地址和当前 ip 所处的状态进行映射存储的容器
  // 这里只所以用 LinkedHashMap 而没有用 Concurrent
  // 是因为多线程操作该对象的时候，分别操作不同的 IP 段，不会在 Map 里面存在两个或以上线上同时读写 同一个 ip key
  private final LinkedHashMap<Integer, State> stateMap;

  // 产生 Invalid 事件的概率
  private final double errorProb;

  public EventGenerator() {
    this(0.0);
  }

  public EventGenerator(double errorProb) {
    this.errorProb = errorProb;
    stateMap = new LinkedHashMap<>();
    random = new Random();
  }

  /**
   * 根据 ip 段来返回一个该段内的 随机 ip 产生的随机事件
   *
   * @param minIp
   * @param maxIp
   * @return
   */
  public Event next(int minIp, int maxIp) {
    double p = random.nextDouble();
    if (p * 1000 >= stateMap.size()) { // 当前容器的随机 IP 和 状态机数量比较少，则随机产生一个初始化的状态机，加入容器
      // 一个 IP 对应一个 State，一个 State 就是一个状态机
      int randomIp = minIp + random.nextInt(maxIp - minIp);
      if (!stateMap.containsKey(randomIp)) {
        // 初始化一个状态机
        EventTypeAndState initialTypeAndState = State.Initial.randomTransition(random);
        // 下面两句话的意思是，这个 ip 发生了 initialTypeAndState.eventType 这个事件，现在处于 initialTypeAndState.state 这个状态。
        stateMap.put(randomIp, initialTypeAndState.state);
        return new Event(initialTypeAndState.eventType, randomIp);
      } else { // ip 地址冲突，重试
        return next(minIp, maxIp);
      }
    } else { // 状态机容器容量过大，清除一个元素并返回该元素，先根据 错误概率判断是否应该返回一个错误事件，
      // 如果是返回正确事件，并根据该元素是否是 Terminal 状态解决是否将该元素重新回炉
      int skipSize = Math.min(20, random.nextInt(stateMap.size()));
      Iterator<Map.Entry<Integer, State>> it = stateMap.entrySet().iterator();
      // 跳过 skipSize 个元素
      for (int i = 0; i < skipSize; i++) {
        it.next();
      }
      // 前面的 random.nextInt(stateMap.size()) 保证迭代器迭代到这里，肯定不会结束
      Map.Entry<Integer, State> currEntry = it.next();
      it.remove();
      State currState = currEntry.getValue();
      int sourceIp = currEntry.getKey();
      if (p < this.errorProb) { // 命中返回异常状态的几率
        EventType transition = currState.randomInvalidTransition(random);
        return new Event(transition, sourceIp);
      } else { // 命中返回正常状态的几率
        EventTypeAndState eventTypeAndState = currState.randomTransition(random);
        if (eventTypeAndState.state != State.Terminal) {
          // reinsert 进行回炉
          stateMap.put(sourceIp, eventTypeAndState.state);
        }
        return new Event(eventTypeAndState.eventType, sourceIp);
      }
    }
  }

  /**
   * 从 mapState 容器中中，跳出第一个元素，获取其装入， 然后将该状态移动到下一个异常状态
   *
   * @return
   */
  public Event nextInvalid() {
    if (stateMap.isEmpty()) {
      return null;
    }
    Iterator<Map.Entry<Integer, State>> iterator = stateMap.entrySet().iterator();
    Map.Entry<Integer, State> entry = iterator.next();
    State currState = entry.getValue();
    int currIp = entry.getKey();
    iterator.remove();
    EventType eventType = currState.randomInvalidTransition(random);
    // 让当前 ip 的 产生一个 异常事件
    return new Event(eventType, currIp);
  }

  public int numActiveEntries() {
    return stateMap.size();
  }

}

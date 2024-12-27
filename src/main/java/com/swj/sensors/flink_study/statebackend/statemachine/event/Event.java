package com.swj.sensors.flink_study.statebackend.statemachine.event;

import org.apache.flink.util.Preconditions;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/11 11:35
 */
public class Event {

  private final int sourceAddress;
  private final EventType eventType;

  public Event(EventType eventType, int sourceAddress) {
    this.eventType = Preconditions.checkNotNull(eventType);
    this.sourceAddress = sourceAddress;
  }

  public int sourceAddress() {
    return sourceAddress;
  }

  public EventType type() {
    return this.eventType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Event event = (Event) o;
    return sourceAddress == event.sourceAddress &&
        eventType == event.eventType;
  }

  @Override
  public int hashCode() {
    return 31 * eventType.hashCode() + sourceAddress;
  }

  @Override
  public String toString() {
    return "Event{" +
        "sourceAddress=" + formatAddress(sourceAddress) +
        ", eventType=" + eventType.name() +
        '}';
  }

  /**
   * 将 int32 格式化为 一个 ip 地址的形式
   *
   * @param address
   * @return
   */
  public static String formatAddress(int address) {
    int p1 = (address >>> 24) & 0xff;
    int p2 = (address >>> 16) & 0xff;
    int p3 = (address >>> 8) & 0xff;
    int p4 = address & 0xff;
    return p1 + "." + p2 + "." + p3 + "." + p4;
  }
}

package com.swj.sensors.flink_study.statebackend.statemachine.kafka;

import com.swj.sensors.flink_study.statebackend.statemachine.event.Event;
import com.swj.sensors.flink_study.statebackend.statemachine.event.EventType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/12 11:16
 */
public class EventDeserializer implements SerializationSchema<Event>, DeserializationSchema<Event> {

  private static final long serialVersionUID = 1L;

  @Override
  public Event deserialize(byte[] message) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN);
    int address = byteBuffer.getInt(0);
    int evTypeOrdinal = byteBuffer.getInt(4);
    return new Event(EventType.values()[evTypeOrdinal], address);
  }

  @Override
  public boolean isEndOfStream(Event nextElement) {
    return false;
  }

  @Override
  public byte[] serialize(Event element) {
    // ByteOrder.LITTLE_ENDIAN 小字节方法，低字节存储在内存低位
    ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(0, element.sourceAddress());
    buffer.putInt(4, element.type().ordinal());
    return buffer.array();
  }

  @Override
  public TypeInformation<Event> getProducedType() {
    return TypeInformation.of(Event.class);
  }
}

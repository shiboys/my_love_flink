package com.swj.sensors.flink_study.statebackend.broadcaststate;

import com.alibaba.fastjson.JSON;
import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Action;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/07 19:54
 */
public class ActionEventSchema implements SerializationSchema<Action>, DeserializationSchema<Action> {
  @Override
  public byte[] serialize(Action element) {
    String str = JSON.toJSONString(element);
    return str.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Action deserialize(byte[] message) throws IOException {
    JSON.parseObject(message, Action.class);
    return null;
  }

  @Override
  public boolean isEndOfStream(Action nextElement) {
    return false;
  }

  @Override
  public TypeInformation<Action> getProducedType() {
    return TypeInformation.of(new TypeHint<Action>() {});
  }
}

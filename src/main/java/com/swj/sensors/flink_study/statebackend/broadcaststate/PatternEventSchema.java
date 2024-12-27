package com.swj.sensors.flink_study.statebackend.broadcaststate;

import com.alibaba.fastjson.JSON;
import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Action;
import com.swj.sensors.flink_study.statebackend.broadcaststate.model.Pattern;
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
public class PatternEventSchema implements SerializationSchema<Pattern>, DeserializationSchema<Pattern> {

  @Override
  public byte[] serialize(Pattern element) {
    String str = JSON.toJSONString(element);
    return str.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Pattern deserialize(byte[] message) throws IOException {
    return JSON.parseObject(message,Pattern.class);
  }

  @Override
  public boolean isEndOfStream(Pattern nextElement) {
    return false;
  }

  @Override
  public TypeInformation<Pattern> getProducedType() {
    return TypeInformation.of(new TypeHint<Pattern>() {
    });
  }
}

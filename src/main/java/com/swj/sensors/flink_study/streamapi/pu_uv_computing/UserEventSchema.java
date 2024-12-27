package com.swj.sensors.flink_study.streamapi.pu_uv_computing;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/02 15:10
 */
public class UserEventSchema
    implements SerializationSchema<UserBehaviorEvent>, DeserializationSchema<UserBehaviorEvent> {

  private static final long serialVersionUID = 6154188370181669758L;

  @Override
  public UserBehaviorEvent deserialize(byte[] message) throws IOException {
    return JSON.parseObject(message, UserBehaviorEvent.class);
  }

  @Override
  public boolean isEndOfStream(UserBehaviorEvent nextElement) {
    return false;
  }

  @Override
  public byte[] serialize(UserBehaviorEvent element) {
    String serializedStr = JSON.toJSONString(element);
    return serializedStr.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public TypeInformation<UserBehaviorEvent> getProducedType() {
    return TypeInformation.of(new TypeHint<UserBehaviorEvent>() {
    });
  }

//  public static void main(String[] args) {
//    UserBehaviorEvent u1 = new UserBehaviorEvent();
//    u1.setAction("buy");
//    u1.setCategoryId("fruit");
//    u1.setItemId(11);
//    u1.setUserId(1);
//    u1.setTs(System.currentTimeMillis());
//    UserEventSchema schema = new UserEventSchema();
//    try {
//      UserBehaviorEvent u2 = schema.deserialize(schema.serialize(u1));
//      System.out.println(u2);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
}

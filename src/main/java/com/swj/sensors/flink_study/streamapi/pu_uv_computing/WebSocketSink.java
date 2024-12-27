package com.swj.sensors.flink_study.streamapi.pu_uv_computing;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/02 14:50
 */
public class WebSocketSink extends RichSinkFunction<Tuple4<Long, Long, Long, Integer>> {

  private static WebSocketClient webSocketClient;

  private String webSocketUrl;

  public WebSocketSink(String webSocketUrl) {
    this.webSocketUrl = webSocketUrl;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    if (webSocketClient == null) {
      webSocketClient = new WebSocketClient(webSocketUrl);
      webSocketClient.init();
    }
  }

  @Override
  public void invoke(Tuple4<Long, Long, Long, Integer> value, Context context) throws Exception {
    Map<String, Object> tupleMap = new HashMap<>();
    tupleMap.put("window_start", value.f0);
    tupleMap.put("window_end", value.f1);
    tupleMap.put("pv", value.f2);
    tupleMap.put("uv", value.f3);
    webSocketClient.sendMessage(JSON.toJSONString(tupleMap));
  }

  @Override
  public void close() throws Exception {
    if (webSocketClient != null) {
      webSocketClient.close();
    }
  }
}

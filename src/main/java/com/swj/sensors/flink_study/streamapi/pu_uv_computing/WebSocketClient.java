package com.swj.sensors.flink_study.streamapi.pu_uv_computing;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import javax.websocket.ClientEndpoint;
import javax.websocket.ContainerProvider;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

/**
 * @author shiweijie
 * @version 1.0.0
 * @since 2023/12/02 11:06
 */

@Slf4j
@ClientEndpoint
public class WebSocketClient {
  private Session session;
  private URI uri;

  public WebSocketClient(String uri) {
    try {
      this.uri = new URI(uri);
    } catch (URISyntaxException e) {
      log.error("WebSocket 地址错误", e);
    }
  }

  @OnOpen
  public void open(Session session) {
    log.info("webSocket 开启会话");
    this.session = session;
  }

  @OnMessage
  public void onMessage(String message) {
    log.info("webSocket 接收到信息：" + message);
  }

  @OnError
  public void onError(Session session, Throwable error) {
    log.error("webSocket  发生错误. ", error);
  }

  public void onClose() {
    log.info("webSocket 关闭");
  }



  public void sendMessage(String message) {
    if (this.session == null || !this.session.isOpen()) {
      log.error("webSocket 状态异常，不能发送信息，请稍后重试。。。");
      this.init();
    } else {
      this.session.getAsyncRemote().sendText(message);
    }
  }

  public void init() {
    // web socket 的 container provider 实现 是 tyrus-standalone-client
    WebSocketContainer container = ContainerProvider.getWebSocketContainer();
    try {
      container.connectToServer(this, uri);
    } catch (Exception e) {
      log.error("连接 webSocket server 异常。", e);
    }
  }

  public void close() {
    if (session != null) {
      try {
        session.close();
      } catch (IOException e) {
        log.error("webSocket session 关闭异常", e);
      }
    }
  }

  public static void main(String[] args) throws InterruptedException {
    String uri = "ws://localhost:8088/websocket/server";
    WebSocketClient webSocketClient = new WebSocketClient(uri);
    webSocketClient.init();
    int counter = 0;
    while (counter < 100) {
      Map<String, Object> websocketMessage = new HashMap<>();
      websocketMessage.put("toGroupIds", new String[] {"html"});
      websocketMessage.put("toClientIds", new String[] {});
      websocketMessage.put("channel", "test_data_alter");
      websocketMessage.put("message", "1231234");
      webSocketClient.sendMessage(JSON.toJSONString(websocketMessage));
      log.info(String.valueOf(++counter));
      Thread.sleep(1000);
    }

    webSocketClient.close();
  }

}

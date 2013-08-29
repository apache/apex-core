/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api.util;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Abstract PubSubWebSocketClient class.</p>
 *
 * @since 0.3.2
 */
public abstract class PubSubWebSocketClient
{
  private static final WebSocketClientFactory factory = new WebSocketClientFactory();
  private WebSocketClient client;
  private WebSocket.Connection connection;
  private ObjectMapper mapper = (new JacksonObjectMapperProvider()).getContext(null);
  private URI uri;

  private class PubSubWebSocket implements WebSocket.OnTextMessage
  {
    @Override
    public void onMessage(String message)
    {
      LOG.debug("onMessage {}", message);
      try {
        @SuppressWarnings("unchecked")
        HashMap<String, Object> map = mapper.readValue(message, HashMap.class);
        PubSubWebSocketClient.this.onMessage((String)map.get("type"), (String)map.get("topic"), map.get("data"));
      }
      catch (Exception ex) {
        LOG.error("onMessage has problem parsing message {}", message, ex);
      }
    }

    @Override
    public void onOpen(WebSocket.Connection connection)
    {
      LOG.debug("WebSocket connection opened");
      PubSubWebSocketClient.this.onOpen(connection);
    }

    @Override
    public void onClose(int code, String message)
    {
      LOG.warn("WebSocket connection has closed with code {}, message {}", code, message);
      PubSubWebSocketClient.this.onClose(code, message);
    }

  }

  /**
   * <p>Constructor for PubSubWebSocketClient.</p>
   */
  public PubSubWebSocketClient()
  {
    try {
      client = factory.newWebSocketClient();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * <p>Setter for the field <code>uri</code>.</p>
   */
  public void setUri(URI uri)
  {
    this.uri = uri;
  }

  /**
   * <p>openConnection.</p>
   */
  public void openConnection(long timeoutMillis) throws IOException, ExecutionException, InterruptedException, TimeoutException
  {
    connection = client.open(uri, new PubSubWebSocket()).get(timeoutMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * <p>isConnectionOpen.</p>
   */
  public boolean isConnectionOpen()
  {
    return connection != null && connection.isOpen();
  }

  /**
   * <p>constructPublishMessage.</p>
   */
  public static String constructPublishMessage(String topic, Object data, ObjectMapper mapper) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "publish");
    map.put("topic", topic);
    map.put("data", data);

    return mapper.writeValueAsString(map);
  }

  /**
   * <p>publish.</p>
   */
  public void publish(String topic, Object data) throws IOException
  {
    connection.sendMessage(constructPublishMessage(topic, data, mapper));
  }

  /**
   * <p>constructSubscribeMessage.</p>
   */
  public static String constructSubscribeMessage(String topic, ObjectMapper mapper) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "subscribe");
    map.put("topic", topic);

    return mapper.writeValueAsString(map);
  }

  /**
   * <p>subscribe.</p>
   */
  public void subscribe(String topic) throws IOException
  {
    connection.sendMessage(constructSubscribeMessage(topic, mapper));
  }

  /**
   * <p>constructUnsubscribeMessage.</p>
   */
  public static String constructUnsubscribeMessage(String topic, ObjectMapper mapper) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "unsubscribe");
    map.put("topic", topic);

    return mapper.writeValueAsString(map);
  }

  /**
   * <p>unsubscribe.</p>
   */
  public void unsubscribe(String topic) throws IOException
  {
    connection.sendMessage(constructUnsubscribeMessage(topic, mapper));
  }

  /**
   * <p>constructSubscribeNumSubscribersMessage.</p>
   */
  public static String constructSubscribeNumSubscribersMessage(String topic, ObjectMapper mapper) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "subscribeNumSubscribers");
    map.put("topic", topic);

    return mapper.writeValueAsString(map);
  }

  /**
   * <p>subscribeNumSubscribers.</p>
   */
  public void subscribeNumSubscribers(String topic) throws IOException
  {
    connection.sendMessage(constructSubscribeNumSubscribersMessage(topic, mapper));
  }

  /**
   * <p>constructUnsubscribeNumSubscribersMessage.</p>
   */
  public static String constructUnsubscribeNumSubscribersMessage(String topic, ObjectMapper mapper) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "unsubscribeNumSubscribers");
    map.put("topic", topic);

    return mapper.writeValueAsString(map);
  }

  /**
   * <p>unsubscribeNumSubscribers.</p>
   */
  public void unsubscribeNumSubscribers(String topic) throws IOException
  {
    connection.sendMessage(constructUnsubscribeNumSubscribersMessage(topic, mapper));
  }

  /**
   * <p>onOpen.</p>
   */
  public abstract void onOpen(WebSocket.Connection connection);

  /**
   * <p>onMessage.</p>
   */
  public abstract void onMessage(String type, String topic, Object data);

  /**
   * <p>onClose.</p>
   */
  public abstract void onClose(int code, String message);

  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketClient.class);

  static {
    try {
      factory.start();
    }
    catch (Exception ex) {
      LOG.warn("WebSocket initialization failure", ex);
    }
  }

}

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

import com.datatorrent.api.util.PubSubMessage.PubSubMessageType;
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
  private PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
  private URI uri;

  private class PubSubWebSocket implements WebSocket.OnTextMessage
  {
    @Override
    public void onMessage(String message)
    {
      LOG.debug("onMessage {}", message);
      try {
        @SuppressWarnings("unchecked")
        PubSubMessage<Object> pubSubMessage = codec.parseMessage(message);
        PubSubWebSocketClient.this.onMessage(pubSubMessage.getType().getIdentifier(), pubSubMessage.getTopic(), pubSubMessage.getData());
      }
      catch (Exception ex) {
        ex.printStackTrace();
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
   * @deprecated
   */
  public static String constructPublishMessage(String topic, Object data, ObjectMapper mapper) throws IOException
  {
    PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
    return constructPublishMessage(topic, data, codec);
  }

  /**
   * <p>constructPublishMessage.</p>
   */
  public static <T> String constructPublishMessage(String topic, T data, PubSubMessageCodec<T> codec) throws IOException {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.PUBLISH);
    pubSubMessage.setTopic(topic);
    pubSubMessage.setData(data);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>publish.</p>
   */
  public void publish(String topic, Object data) throws IOException
  {
    connection.sendMessage(constructPublishMessage(topic, data, codec));
  }

  /**
   * <p>constructSubscribeMessage.</p>
   * @deprecated
   */
  public static String constructSubscribeMessage(String topic, ObjectMapper mapper) throws IOException
  {
    PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
    return constructSubscribeMessage(topic, codec);
  }

  /**
   * <p>constructSubscribeMessage.</p>
   */
  public static <T> String constructSubscribeMessage(String topic, PubSubMessageCodec<T> codec) throws IOException {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.SUBSCRIBE);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>subscribe.</p>
   */
  public void subscribe(String topic) throws IOException
  {
    connection.sendMessage(constructSubscribeMessage(topic, codec));
  }

  /**
   * <p>constructUnsubscribeMessage.</p>
   * @deprecated
   */
  public static String constructUnsubscribeMessage(String topic, ObjectMapper mapper) throws IOException
  {
    PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
    return constructUnsubscribeMessage(topic, codec);
  }

  /**
   * <p>constructUnsubscribeMessage.</p>
   */
  public static <T> String constructUnsubscribeMessage(String topic, PubSubMessageCodec<T> codec) throws IOException {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.UNSUBSCRIBE);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>unsubscribe.</p>
   */
  public void unsubscribe(String topic) throws IOException
  {
    connection.sendMessage(constructUnsubscribeMessage(topic, codec));
  }

  /**
   * <p>constructSubscribeNumSubscribersMessage.</p>
   * @deprecated
   */
  public static String constructSubscribeNumSubscribersMessage(String topic, ObjectMapper mapper) throws IOException
  {
    PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
    return constructSubscribeNumSubscribersMessage(topic, codec);
  }

  /**
   * <p>constructSubscribeNumSubscribersMessage.</p>
   */
  public static <T> String constructSubscribeNumSubscribersMessage(String topic, PubSubMessageCodec<T> codec) throws IOException {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.SUBSCRIBE_NUM_SUBSCRIBERS);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>subscribeNumSubscribers.</p>
   */
  public void subscribeNumSubscribers(String topic) throws IOException
  {
    connection.sendMessage(constructSubscribeNumSubscribersMessage(topic, codec));
  }

  /**
   * <p>constructUnsubscribeNumSubscribersMessage.</p>
   * @deprecated
   */
  public static String constructUnsubscribeNumSubscribersMessage(String topic, ObjectMapper mapper) throws IOException
  {
    PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
    return constructUnsubscribeNumSubscribersMessage(topic, codec);
  }

  /**
   * <p>constructUnsubscribeNumSubscribersMessage.</p>
   */
  public static <T> String constructUnsubscribeNumSubscribersMessage(String topic, PubSubMessageCodec<T> codec) throws IOException {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.UNSUBSCRIBE_NUM_SUBSCRIBERS);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>unsubscribeNumSubscribers.</p>
   */
  public void unsubscribeNumSubscribers(String topic) throws IOException
  {
    connection.sendMessage(constructUnsubscribeNumSubscribersMessage(topic, codec));
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

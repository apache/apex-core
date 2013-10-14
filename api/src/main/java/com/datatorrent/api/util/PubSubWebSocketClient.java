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

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.websocket.WebSocket;
import com.ning.http.client.websocket.WebSocketTextListener;
import com.ning.http.client.websocket.WebSocketUpgradeHandler;
import com.datatorrent.api.util.PubSubMessage.PubSubMessageType;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Abstract PubSubWebSocketClient class.</p>
 *
 * @since 0.3.2
 */
public abstract class PubSubWebSocketClient
{
  private AsyncHttpClient client;
  private WebSocket connection;
  private ObjectMapper mapper = (new JacksonObjectMapperProvider()).getContext(null);
  private PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
  private URI uri;

  private class PubSubWebSocket implements WebSocketTextListener
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
        LOG.error("onMessage has problem parsing message {}", message, ex);
      }
    }

    @Override
    public void onFragment(String fragment, boolean last)
    {
    }

    @Override
    public void onOpen(WebSocket ws)
    {
      LOG.debug("WebSocket connection opened");
      PubSubWebSocketClient.this.onOpen(ws);
    }

    @Override
    public void onClose(WebSocket ws)
    {
      LOG.info("WebSocket connection has closed");
      PubSubWebSocketClient.this.onClose(ws);
    }

    @Override
    public void onError(Throwable t)
    {
      LOG.error("WebSocket connection has an error", t);
    }
  }

  /**
   * <p>Constructor for PubSubWebSocketClient.</p>
   */
  public PubSubWebSocketClient()
  {
    try {
      client = new AsyncHttpClient();
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
    connection = client.prepareGet(uri.toString()).execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new PubSubWebSocket()).build()).get(timeoutMillis, TimeUnit.MILLISECONDS);
  }

  public void openConnectionAsync() throws IOException
  {
    client.prepareGet(uri.toString()).execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new PubSubWebSocket() {

      @Override
      public void onOpen(WebSocket ws)
      {
        connection = ws;
        super.onOpen(ws);
      }

    }).build());
  }

  /**
   * <p>isConnectionOpen.</p>
   * @return
   */
  public boolean isConnectionOpen()
  {
    return connection != null && connection.isOpen();
  }

  /**
   * <p>constructPublishMessage.</p>
   * @param topic
   * @param mapper
   * @param data
   * @return
   * @throws IOException
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
   * @param topic
   * @param data
   * @throws IOException
   */
  public void publish(String topic, Object data) throws IOException
  {
    connection.sendTextMessage(constructPublishMessage(topic, data, codec));
  }

  /**
   * <p>constructSubscribeMessage.</p>
   * @param topic
   * @param mapper
   * @return
   * @throws IOException
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
   * @param topic
   * @throws IOException
   */
  public void subscribe(String topic) throws IOException
  {
    connection.sendTextMessage(constructSubscribeMessage(topic, codec));
  }

  /**
   * <p>constructUnsubscribeMessage.</p>

   * @param topic
   * @param mapper
   * @return
   * @throws IOException
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
   * @param topic
   * @throws IOException
   */
  public void unsubscribe(String topic) throws IOException
  {
    connection.sendTextMessage(constructUnsubscribeMessage(topic, codec));
  }

  /**
   * <p>constructSubscribeNumSubscribersMessage.</p>
   * @param topic
   * @param mapper
   * @return
   * @throws IOException
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
   * @param topic
   * @throws IOException
   */
  public void subscribeNumSubscribers(String topic) throws IOException
  {
    connection.sendTextMessage(constructSubscribeNumSubscribersMessage(topic, codec));
  }

  /**
   * <p>constructUnsubscribeNumSubscribersMessage.</p>
   * @param topic
   * @param mapper
   * @return
   * @throws IOException
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
   * @param topic
   * @throws IOException
   */
  public void unsubscribeNumSubscribers(String topic) throws IOException
  {
    connection.sendTextMessage(constructUnsubscribeNumSubscribersMessage(topic, codec));
  }

  /**
   * <p>onOpen.</p>
   * @param ws
   */
  public abstract void onOpen(WebSocket ws);

  /**
   * <p>onMessage.</p>
   * @param type
   * @param topic
   * @param data
   */
  public abstract void onMessage(String type, String topic, Object data);

  /**
   * <p>onClose.</p>
   * @param ws
   */
  public abstract void onClose(WebSocket ws);

  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketClient.class);

}

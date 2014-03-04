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
package com.datatorrent.lib.util;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.*;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfigBean;
import com.ning.http.client.websocket.WebSocket;
import com.ning.http.client.websocket.WebSocketTextListener;
import com.ning.http.client.websocket.WebSocketUpgradeHandler;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.PubSubMessage.PubSubMessageType;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;

import com.datatorrent.common.util.NameableThreadFactory;

/**
 * <p>Abstract PubSubWebSocketClient class.</p>
 *
 * @since 0.3.2
 */
public abstract class PubSubWebSocketClient implements Component<Context>
{
  private AsyncHttpClient client;
  private WebSocket connection;
  private final ObjectMapper mapper = (new JacksonObjectMapperProvider()).getContext(null);
  private final PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
  private URI uri;
  private int ioThreadMultiplier = 1;

  private class PubSubWebSocket implements WebSocketTextListener
  {
    @Override
    public void onMessage(String message)
    {
      LOG.debug("onMessage {}", message);
      try {
        PubSubMessage<Object> pubSubMessage = codec.parseMessage(message);
        PubSubWebSocketClient.this.onMessage(pubSubMessage.getType().getIdentifier(), pubSubMessage.getTopic(), pubSubMessage.getData());
      }
      catch (Exception ex) {
        LOG.warn("onMessage has problem parsing this message \"{}\". Ignoring...", message);
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
      AsyncHttpClientConfigBean config = new AsyncHttpClientConfigBean();
      config.setIoThreadMultiplier(ioThreadMultiplier);
      config.setApplicationThreadPool(Executors.newCachedThreadPool(new NameableThreadFactory("AsyncHttpClient")));
      client = new AsyncHttpClient(config);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * <p>Setter for the field
   * <code>uri</code>.</p>
   *
   * @param uri
   */
  public void setUri(URI uri)
  {
    this.uri = uri;
  }

  public void setIoThreadMultiplier(int ioThreadMultiplier)
  {
    this.ioThreadMultiplier = ioThreadMultiplier;
  }

  /**
   * <p>openConnection.</p>
   *
   * @param timeoutMillis
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  public void openConnection(long timeoutMillis) throws IOException, ExecutionException, InterruptedException, TimeoutException
  {
    connection = client.prepareGet(uri.toString()).execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new PubSubWebSocket()).build()).get(timeoutMillis, TimeUnit.MILLISECONDS);
  }

  public void openConnectionAsync() throws IOException
  {
    client.prepareGet(uri.toString()).execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new PubSubWebSocket()
    {
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
   *
   * @return
   */
  public boolean isConnectionOpen()
  {
    return connection != null && connection.isOpen();
  }

  /**
   * <p>constructPublishMessage.</p>
   *
   * @param topic
   * @param mapper
   * @param data
   * @return
   * @throws IOException
   * @deprecated
   */
  @Deprecated
  public static String constructPublishMessage(String topic, Object data, ObjectMapper mapper) throws IOException
  {
    PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
    return constructPublishMessage(topic, data, codec);
  }

  /**
   * <p>constructPublishMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param data
   * @param codec
   * @return
   * @throws IOException
   */
  public static <T> String constructPublishMessage(String topic, T data, PubSubMessageCodec<T> codec) throws IOException
  {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.PUBLISH);
    pubSubMessage.setTopic(topic);
    pubSubMessage.setData(data);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>publish.</p>
   *
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
   *
   * @param topic
   * @param mapper
   * @return
   * @throws IOException
   * @deprecated
   */
  @Deprecated
  public static String constructSubscribeMessage(String topic, ObjectMapper mapper) throws IOException
  {
    PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
    return constructSubscribeMessage(topic, codec);
  }

  /**
   * <p>constructSubscribeMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return
   * @throws IOException
   */
  public static <T> String constructSubscribeMessage(String topic, PubSubMessageCodec<T> codec) throws IOException
  {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.SUBSCRIBE);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>subscribe.</p>
   *
   * @param topic
   * @throws IOException
   */
  public void subscribe(String topic) throws IOException
  {
    connection.sendTextMessage(constructSubscribeMessage(topic, codec));
  }

  /**
   * <p>constructUnsubscribeMessage.</p>
   *
   * @param topic
   * @param mapper
   * @return
   * @throws IOException
   * @deprecated
   */
  @Deprecated
  public static String constructUnsubscribeMessage(String topic, ObjectMapper mapper) throws IOException
  {
    PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
    return constructUnsubscribeMessage(topic, codec);
  }

  /**
   * <p>constructUnsubscribeMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return
   * @throws IOException
   */
  public static <T> String constructUnsubscribeMessage(String topic, PubSubMessageCodec<T> codec) throws IOException
  {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.UNSUBSCRIBE);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>unsubscribe.</p>
   *
   * @param topic
   * @throws IOException
   */
  public void unsubscribe(String topic) throws IOException
  {
    connection.sendTextMessage(constructUnsubscribeMessage(topic, codec));
  }

  /**
   * <p>constructSubscribeNumSubscribersMessage.</p>
   *
   * @param topic
   * @param mapper
   * @return
   * @throws IOException
   * @deprecated
   */
  @Deprecated
  public static String constructSubscribeNumSubscribersMessage(String topic, ObjectMapper mapper) throws IOException
  {
    PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
    return constructSubscribeNumSubscribersMessage(topic, codec);
  }

  /**
   * <p>constructSubscribeNumSubscribersMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return
   * @throws IOException
   */
  public static <T> String constructSubscribeNumSubscribersMessage(String topic, PubSubMessageCodec<T> codec) throws IOException
  {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.SUBSCRIBE_NUM_SUBSCRIBERS);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>subscribeNumSubscribers.</p>
   *
   * @param topic
   * @throws IOException
   */
  public void subscribeNumSubscribers(String topic) throws IOException
  {
    connection.sendTextMessage(constructSubscribeNumSubscribersMessage(topic, codec));
  }

  /**
   * <p>constructUnsubscribeNumSubscribersMessage.</p>
   *
   * @param topic
   * @param mapper
   * @return
   * @throws IOException
   * @deprecated
   */
  @Deprecated
  public static String constructUnsubscribeNumSubscribersMessage(String topic, ObjectMapper mapper) throws IOException
  {
    PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
    return constructUnsubscribeNumSubscribersMessage(topic, codec);
  }

  /**
   * <p>constructUnsubscribeNumSubscribersMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return
   * @throws IOException
   */
  public static <T> String constructUnsubscribeNumSubscribersMessage(String topic, PubSubMessageCodec<T> codec) throws IOException
  {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.UNSUBSCRIBE_NUM_SUBSCRIBERS);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>unsubscribeNumSubscribers.</p>
   *
   * @param topic
   * @throws IOException
   */
  public void unsubscribeNumSubscribers(String topic) throws IOException
  {
    connection.sendTextMessage(constructUnsubscribeNumSubscribersMessage(topic, codec));
  }

  /**
   * <p>onOpen.</p>
   *
   * @param ws
   */
  public abstract void onOpen(WebSocket ws);

  /**
   * <p>onMessage.</p>
   *
   * @param type
   * @param topic
   * @param data
   */
  public abstract void onMessage(String type, String topic, Object data);

  /**
   * <p>onClose.</p>
   *
   * @param ws
   */
  public abstract void onClose(WebSocket ws);

  @Override
  public void setup(Context context)
  {
  }

  @Override
  public void teardown()
  {
    if (client != null) {
      client.close();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketClient.class);
}

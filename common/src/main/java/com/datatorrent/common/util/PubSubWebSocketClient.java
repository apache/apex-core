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
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import com.ning.http.client.*;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;
import com.ning.http.client.websocket.*;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.PubSubMessage.PubSubMessageType;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.common.util.NameableThreadFactory;

/**
 * <p>Abstract PubSubWebSocketClient class.</p>
 *
 * @since 0.3.2
 */
public abstract class PubSubWebSocketClient implements Component<Context>
{
  private final AsyncHttpClient client;
  private WebSocket connection;
  private final ObjectMapper mapper;
  private final PubSubMessageCodec<Object> codec;
  private URI uri;
  private int ioThreadMultiplier;
  private String loginUrl;
  private String userName;
  private String password;

  private final AtomicReference<Throwable> throwable;

  private class PubSubWebSocket implements WebSocketTextListener
  {
    @Override
    public void onMessage(String message)
    {
      PubSubMessage<Object> pubSubMessage;
      try {
        pubSubMessage = codec.parseMessage(message);
        PubSubWebSocketClient.this.onMessage(pubSubMessage.getType().getIdentifier(), pubSubMessage.getTopic(), pubSubMessage.getData());
      }
      catch (JsonParseException jpe) {
        logger.warn("Ignoring unparseable JSON message: {}", message, jpe);
      }
      catch (JsonMappingException jme) {
        logger.warn("Ignoring JSON mapping in message: {}", message, jme);
      }
      catch (IOException ex) {
        onError(ex);
      }
    }

    @Override
    public void onFragment(String fragment, boolean last)
    {
    }

    @Override
    public void onOpen(WebSocket ws)
    {
      PubSubWebSocketClient.this.onOpen(ws);
    }

    @Override
    public void onClose(WebSocket ws)
    {
      PubSubWebSocketClient.this.onClose(ws);
    }

    @Override
    public void onError(Throwable t)
    {
      PubSubWebSocketClient.this.onError(t);
    }

  }

  /**
   * <p>Constructor for PubSubWebSocketClient.</p>
   */
  public PubSubWebSocketClient()
  {
    throwable = new AtomicReference<Throwable>();
    ioThreadMultiplier = 1;
    mapper = (new JacksonObjectMapperProvider()).getContext(null);
    codec = new PubSubMessageCodec<Object>(mapper);

    AsyncHttpClientConfigBean config = new AsyncHttpClientConfigBean();
    config.setIoThreadMultiplier(ioThreadMultiplier);
    config.setApplicationThreadPool(Executors.newCachedThreadPool(new NameableThreadFactory("AsyncHttpClient")));
    client = new AsyncHttpClient(config);
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

  public void setLoginUrl(String url)
  {
    this.loginUrl = url;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public void setPassword(String password)
  {
    this.password = password;
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
    throwable.set(null);

    List<Cookie> cookies = null;
    if (loginUrl != null && userName != null && password != null) {
      // get the session key first before attempting web socket
      JSONObject json = new JSONObject();
      try {
        json.put("userName", userName);
        json.put("password", password);
      }
      catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
      Response response = client.preparePost(loginUrl).setHeader("Content-Type", "application/json").setBody(json.toString()).execute().get();
      cookies = response.getCookies();
    }
    BoundRequestBuilder brb = client.prepareGet(uri.toString());
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        brb.addCookie(cookie);
      }
    }
    connection = brb.execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new PubSubWebSocket()).build()).get(timeoutMillis, TimeUnit.MILLISECONDS);
  }

  public void openConnectionAsync() throws IOException
  {
    throwable.set(null);

    if (loginUrl != null && userName != null && password != null) {
      // get the session key first before attempting web socket
      JSONObject json = new JSONObject();
      try {
        json.put("userName", userName);
        json.put("password", password);
      }
      catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
      client.preparePost(loginUrl).setHeader("Content-Type", "application/json").setBody(json.toString()).execute(new AsyncCompletionHandler<Response>()
      {

        @Override
        public Response onCompleted(Response response) throws Exception
        {
          List<Cookie> cookies = response.getCookies();
          BoundRequestBuilder brb = client.prepareGet(uri.toString());
          if (cookies != null) {
            for (Cookie cookie : cookies) {
              brb.addCookie(cookie);
            }
          }
          connection = brb.execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new PubSubWebSocket()).build()).get();
          return response;
        }

      });
    }
    else {
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
  }

  /**
   *
   * @return true if the connection is open; false otherwise.
   */
  public boolean isConnectionOpen()
  {
    if (connection == null) {
      return false;
    }

    return connection.isOpen();
  }

  /**
   * <p>constructPublishMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param data
   * @param codec
   * @return publish message.
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
   * Before the websocket is used, it's recommended to call this method to ensure that
   * any exceptions caught while processing the messages received are acknowledged.
   * @throws IOException The reason because of which connection cannot be used.
   */
  public void assertUsable() throws IOException
  {
    if (throwable.get() == null) {
      if (connection == null) {
        throw new IOException("Connection is not open");
      }
      return;
    }


    Throwable t = throwable.get();
    if (t instanceof IOException) {
      throw (IOException)t;
    }
    else {
      DTThrowable.rethrow(t);
    }
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
    assertUsable();
    connection.sendTextMessage(constructPublishMessage(topic, data, codec));
  }

  /**
   * <p>constructSubscribeMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return subscribe message.
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
    assertUsable();
    connection.sendTextMessage(constructSubscribeMessage(topic, codec));
  }

  /**
   * <p>constructUnsubscribeMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return un-subscribe message.
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
    assertUsable();
    connection.sendTextMessage(constructUnsubscribeMessage(topic, codec));
  }

  /**
   * <p>constructSubscribeNumSubscribersMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return subscribe num subscriber message.
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
    assertUsable();
    connection.sendTextMessage(constructSubscribeNumSubscribersMessage(topic, codec));
  }

  /**
   * <p>constructUnsubscribeNumSubscribersMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return un-subscribe num subscribers message.
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
    assertUsable();
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
    if (connection != null) {
      connection.close();
    }
    if (client != null) {
      client.close();
    }

    throwable.set(null);
  }

  private void onError(Throwable t)
  {
    throwable.set(t);
  }

  private static final Logger logger = LoggerFactory.getLogger(PubSubWebSocketClient.class);
}

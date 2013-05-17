/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

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
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class PubSubWebSocketClient
{
  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketClient.class);
  private static final WebSocketClientFactory factory = new WebSocketClientFactory();
  private WebSocketClient client;
  private WebSocket.Connection connection;
  private ObjectMapper mapper = (new JacksonObjectMapperProvider()).getContext(null);
  private URI uri;

  static {
    try {
      factory.start();
    }
    catch (Exception ex) {
      LOG.warn("WebSocket initialization failure", ex);
    }
  }

  private class PubSubWebSocket implements WebSocket.OnTextMessage
  {
    @Override
    public void onMessage(String message)
    {
      LOG.debug("onMessage {}", message);
      try {
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

  public PubSubWebSocketClient()
  {
    try {
      client = factory.newWebSocketClient();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void setUri(URI uri)
  {
    this.uri = uri;
  }

  public void openConnection(long timeoutMillis) throws IOException, ExecutionException, InterruptedException, TimeoutException
  {
    connection = client.open(uri, new PubSubWebSocket()).get(timeoutMillis, TimeUnit.MILLISECONDS);
  }

  public boolean isConnectionOpen()
  {
    return connection != null && connection.isOpen();
  }

  public static String constructPublishMessage(String topic, Object data, ObjectMapper mapper) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "publish");
    map.put("topic", topic);
    map.put("data", data);

    return mapper.writeValueAsString(map);
  }

  public void publish(String topic, Object data) throws IOException
  {
    connection.sendMessage(constructPublishMessage(topic, data, mapper));
  }

  public static String constructSubscribeMessage(String topic, ObjectMapper mapper) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "subscribe");
    map.put("topic", topic);

    return mapper.writeValueAsString(map);
  }

  public void subscribe(String topic) throws IOException
  {
    connection.sendMessage(constructSubscribeMessage(topic, mapper));
  }

  public static String constructUnsubscribeMessage(String topic, ObjectMapper mapper) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "unsubscribe");
    map.put("topic", topic);

    return mapper.writeValueAsString(map);
  }

  public void unsubscribe(String topic) throws IOException
  {
    connection.sendMessage(constructUnsubscribeMessage(topic, mapper));
  }

  public static String constructSubscribeNumSubscribersMessage(String topic, ObjectMapper mapper) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "subscribeNumSubscribers");
    map.put("topic", topic);

    return mapper.writeValueAsString(map);
  }

  public void subscribeNumSubscribers(String topic) throws IOException
  {
    connection.sendMessage(constructSubscribeNumSubscribersMessage(topic, mapper));
  }

  public static String constructUnsubscribeNumSubscribersMessage(String topic, ObjectMapper mapper) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "unsubscribeNumSubscribers");
    map.put("topic", topic);

    return mapper.writeValueAsString(map);
  }

  public void unsubscribeNumSubscribers(String topic) throws IOException
  {
    connection.sendMessage(constructUnsubscribeNumSubscribersMessage(topic, mapper));
  }

  public abstract void onOpen(WebSocket.Connection connection);

  public abstract void onMessage(String type, String topic, Object data);

  public abstract void onClose(int code, String message);

}

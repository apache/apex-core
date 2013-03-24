/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
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
        LOG.error("onMessage has problem parsing message {}", message);
      }
    }

    @Override
    public void onOpen(WebSocket.Connection connection)
    {
      LOG.debug("WebSocket connection opened");
    }

    @Override
    public void onClose(int code, String message)
    {
      LOG.warn("WebSocket connection has closed with code {}, message {}", code, message);
      // try to reconnect
      try {
        connection = client.open(uri, new PubSubWebSocket()).get(5, TimeUnit.SECONDS);
      }
      catch (Exception ex) {
        LOG.warn("Failed to reconnect to {}", uri);
      }
    }

  }

  public PubSubWebSocketClient(URI uri)
  {
    try {
      client = factory.newWebSocketClient();
      connection = client.open(uri, new PubSubWebSocket()).get(5, TimeUnit.SECONDS);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void publish(String topic, Object data) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "publish");
    map.put("topic", topic);
    map.put("data", data);

    String message = mapper.writeValueAsString(map);
    connection.sendMessage(message);
  }

  public void subscribe(String topic) throws IOException
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "subscribe");
    map.put("topic", topic);
    connection.sendMessage(mapper.writeValueAsString(map));
  }

  public abstract void onMessage(String type, String topic, Object data);

}

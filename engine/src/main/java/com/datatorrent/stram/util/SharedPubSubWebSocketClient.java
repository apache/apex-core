/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import com.datatorrent.lib.util.PubSubMessage;
import com.datatorrent.lib.util.PubSubWebSocketClient;
import com.ning.http.client.websocket.WebSocket;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>SharedPubSubWebSocketClient class.</p>
 *
 * @since 0.9.0
 */
public class SharedPubSubWebSocketClient extends PubSubWebSocketClient
{
  public static final String LAST_INDEX_TOPIC_PREFIX = PubSubMessage.INTERNAL_TOPIC_PREFIX + ".lastIndex";
  private static final Logger LOG = LoggerFactory.getLogger(SharedPubSubWebSocketClient.class);
  private final Map<String, List<Handler>> topicHandlers = new HashMap<String, List<Handler>>();
  private long lastConnectTryTime;
  private final long minWaitConnectionRetry = 5000;
  private final long timeoutMillis;

  public interface Handler
  {
    public void onMessage(String type, String topic, Object data);

    public void onClose();

  }

  public SharedPubSubWebSocketClient(String uri, long timeoutMillis) throws URISyntaxException
  {
    this.setUri(new URI(uri));
    lastConnectTryTime = System.currentTimeMillis();
    this.timeoutMillis = timeoutMillis;
  }

  public void setup() throws IOException, ExecutionException, InterruptedException, TimeoutException
  {
    openConnection(timeoutMillis);
  }

  public synchronized void addHandler(String topic, boolean numSubscribers, Handler handler)
  {
    List<Handler> handlers;
    String originalTopic = topic;
    if (numSubscribers) {
      topic += ".numSubscribers";
    }
    if (topicHandlers.containsKey(topic)) {
      handlers = topicHandlers.get(topic);
    }
    else {
      handlers = new ArrayList<Handler>();
      topicHandlers.put(topic, handlers);
    }
    handlers.add(handler);
    try {
      if (isConnectionOpen()) {
        if (numSubscribers) {
          subscribeNumSubscribers(originalTopic);
        }
        else {
          subscribe(topic);
        }
      }
    }
    catch (IOException ex) {
      LOG.warn("Cannot subscribe to {}", topic);
    }
  }

  @Override
  public void publish(String topic, Object data) throws IOException
  {
    if (!isConnectionOpen()) {
      try {
        long now = System.currentTimeMillis();
        if (lastConnectTryTime + minWaitConnectionRetry < now) {
          lastConnectTryTime = now;
          openConnectionAsync();
        }
      }
      catch (Exception ex) {
      }
    }
    super.publish(topic, data);
  }

  @Override
  public void onOpen(WebSocket ws)
  {
    for (String topic : topicHandlers.keySet()) {
      try {
        subscribe(topic);
      }
      catch (IOException ex) {
        LOG.warn("Cannot subscribe to {}", topic);
      }
    }
  }

  @Override
  public synchronized void onMessage(String type, String topic, Object data)
  {
    List<Handler> handlers = topicHandlers.get(topic);
    if (handlers != null) {
      for (Handler handler : handlers) {
        handler.onMessage(type, topic, data);
      }
    }
  }

  @Override
  public void onClose(WebSocket ws)
  {
    for (Map.Entry<String, List<Handler>> entry : topicHandlers.entrySet()) {
      for (Handler handler : entry.getValue()) {
        handler.onClose();
      }
    }
  }

}

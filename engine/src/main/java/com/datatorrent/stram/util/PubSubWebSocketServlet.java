/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.PubSubMessage;
import com.datatorrent.common.util.PubSubMessage.PubSubMessageType;
import com.datatorrent.common.util.PubSubMessageCodec;


/**
 * <p>PubSubWebSocketServlet class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public class PubSubWebSocketServlet<SECURITY_CONTEXT, PRINCIPAL> extends WebSocketServlet
{
  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketServlet.class);
  private static final long serialVersionUID = 1L;
  private HashMap<String, HashSet<PubSubWebSocket>> topicToSocketMap = new HashMap<>();
  private HashMap<PubSubWebSocket, HashSet<String>> socketToTopicMap = new HashMap<>();
  private ObjectMapper mapper = (new JSONSerializationProvider()).getContext(null);
  private PubSubMessageCodec<Object> codec = new PubSubMessageCodec<>(mapper);
  private InternalMessageHandler internalMessageHandler = null;
  private static final int latestTopicCount = 100;
  protected SECURITY_CONTEXT securityContext;
  private SubscribeFilter subscribeFilter;
  private SendFilter sendFilter;
  private String authAttribute;
  private final LRUCache<String, Long> latestTopics = new LRUCache<String, Long>(latestTopicCount, false)
  {
    private static final long serialVersionUID = 20140131L;

    @Override
    public Long put(String key, Long value)
    {
      remove(key); // this is to make the key the most recently inserted entry
      return super.put(key, value);
    }

  };

  public interface SubscribeFilter<SECURITY_CONTEXT, PRINCIPAL>
  {

    /**
     * Returns whether or not the principal is allowed to subscribe to this topic
     *
     * @param securityContext
     * @param principal
     * @param topic
     * @return
     */
    boolean filter(SECURITY_CONTEXT securityContext, PRINCIPAL principal, String topic);
  }

  public interface SendFilter<SECURITY_CONTEXT, PRINCIPAL>
  {

    /**
     * Returns the data it should be sent given the principal
     *
     * @param securityContext
     * @param principal
     * @param topic
     * @param data
     * @return the data it should send to the websocket
     */
    Object filter(SECURITY_CONTEXT securityContext, PRINCIPAL principal, String topic, Object data);
  }

  public void registerSubscribeFilter(SubscribeFilter filter)
  {
    subscribeFilter = filter;
  }

  public void registerSendFilter(SendFilter filter)
  {
    sendFilter = filter;
  }

  public interface InternalMessageHandler
  {
    void onMessage(String topic, Object data);

  }

  public PubSubWebSocketServlet(SECURITY_CONTEXT securityContext, String authAttribute)
  {
    this.securityContext = securityContext;
    this.authAttribute = authAttribute;
  }

  public void setInternalMessageHandler(InternalMessageHandler internalMessageHandler)
  {
    this.internalMessageHandler = internalMessageHandler;
  }

  public class UserHolder
  {
    public String username;
  }

  @Override
  public WebSocket doWebSocketConnect(HttpServletRequest request, String protocol)
  {
    @SuppressWarnings("unchecked")
    PRINCIPAL principal = (PRINCIPAL)request.getAttribute(authAttribute);
    return new PubSubWebSocket(principal);
  }

  private synchronized void subscribe(PubSubWebSocket webSocket, String topic)
  {
    if (subscribeFilter != null && !subscribeFilter.filter(securityContext, webSocket.getPrincipal(), topic)) {
      LOG.warn("Subscribe filter returns false for topic {}, user {}. Ignoring subscribe request", topic, webSocket.getPrincipal());
      return;
    } else {
      LOG.debug("Subscribe is allowed for topic {}, user {}", topic, webSocket.getPrincipal());
    }

    HashSet<PubSubWebSocket> wsSet;
    if (!topicToSocketMap.containsKey(topic)) {
      wsSet = new HashSet<>();
      topicToSocketMap.put(topic, wsSet);
    } else {
      wsSet = topicToSocketMap.get(topic);
    }
    wsSet.add(webSocket);

    HashSet<String> topicSet;
    if (!socketToTopicMap.containsKey(webSocket)) {
      topicSet = new HashSet<>(0);
      socketToTopicMap.put(webSocket, topicSet);
    } else {
      topicSet = socketToTopicMap.get(webSocket);
    }
    topicSet.add(topic);
    publish(topic + "." + PubSubMessage.NUM_SUBSCRIBERS_SUFFIX, getNumSubscribers(topic));
  }

  private synchronized void unsubscribe(PubSubWebSocket webSocket, String topic)
  {
    if (!topicToSocketMap.containsKey(topic)) {
      return;
    }
    HashSet<PubSubWebSocket> wsSet = topicToSocketMap.get(topic);
    wsSet.remove(webSocket);
    if (wsSet.isEmpty()) {
      topicToSocketMap.remove(topic);
    }
    if (!socketToTopicMap.containsKey(webSocket)) {
      return;
    }
    HashSet<String> topicSet = socketToTopicMap.get(webSocket);
    topicSet.remove(topic);
    if (topicSet.isEmpty()) {
      socketToTopicMap.remove(webSocket);
    }
    publish(topic + "." + PubSubMessage.NUM_SUBSCRIBERS_SUFFIX, getNumSubscribers(topic));
  }

  private synchronized void unsubscribeAll(PubSubWebSocket webSocket)
  {
    HashSet<String> topicSet = socketToTopicMap.get(webSocket);
    if (topicSet != null) {
      for (String topic : topicSet) {
        HashSet<PubSubWebSocket> wsSet = topicToSocketMap.get(topic);
        wsSet.remove(webSocket);
        if (wsSet.isEmpty()) {
          topicToSocketMap.remove(topic);
        }
        publish(topic + "." + PubSubMessage.NUM_SUBSCRIBERS_SUFFIX, getNumSubscribers(topic));
      }
      socketToTopicMap.remove(webSocket);
    }
  }

  private synchronized void disconnect(PubSubWebSocket webSocket)
  {
    unsubscribeAll(webSocket);
  }

  public synchronized int getNumSubscribers(String topic)
  {
    HashSet<PubSubWebSocket> wsSet = topicToSocketMap.get(topic);
    return wsSet == null ? 0 : wsSet.size();
  }

  private synchronized void sendData(PubSubWebSocket webSocket, String topic, Object data) throws IOException
  {
    PubSubMessage<Object> pubSubMessage = new PubSubMessage<>();
    pubSubMessage.setType(PubSubMessageType.DATA);
    pubSubMessage.setTopic(topic);
    pubSubMessage.setData(data);
    LOG.debug("Sending data {} to subscriber...", topic);
    webSocket.sendMessage(codec.formatMessage(pubSubMessage));
  }

  public synchronized void publish(String topic, Object data)
  {
    if (!topic.endsWith("." + PubSubMessage.NUM_SUBSCRIBERS_SUFFIX) && !topic.startsWith(PubSubMessage.INTERNAL_TOPIC_PREFIX + ".")) {
      latestTopics.put(topic, System.currentTimeMillis());
    }
    HashSet<PubSubWebSocket> wsSet = topicToSocketMap.get(topic);
    if (wsSet != null) {
      Iterator<PubSubWebSocket> it = wsSet.iterator();
      while (it.hasNext()) {
        PubSubWebSocket socket = it.next();
        try {
          if (sendFilter != null) {
            Object filteredData = sendFilter.filter(securityContext, socket.getPrincipal(), topic, data);
            sendData(socket, topic, filteredData);
          } else {
            sendData(socket, topic, data);
          }
        } catch (Exception ex) {
          LOG.error("Cannot send message", ex);
          it.remove();
          disconnect(socket);
        }
      }
    }
  }

  protected class PubSubWebSocket implements WebSocket.OnTextMessage
  {
    private Connection connection;
    private final BlockingQueue<String> messageQueue = new ArrayBlockingQueue<>(1024);
    private final Thread messengerThread = new Thread(new Messenger());
    private final PRINCIPAL principal;

    public PubSubWebSocket(PRINCIPAL principal)
    {
      this.principal = principal;
    }

    public PRINCIPAL getPrincipal()
    {
      return principal;
    }

    @Override
    public void onMessage(String message)
    {
      LOG.debug("Received message {}", message);
      try {
        @SuppressWarnings("unchecked")
        PubSubMessage<Object> pubSubMessage = codec.parseMessage(message);
        if (pubSubMessage != null) {
          PubSubMessageType type = pubSubMessage.getType();
          String topic = pubSubMessage.getTopic();
          if (type != null) {
            if (type.equals(PubSubMessageType.SUBSCRIBE)) {
              if (topic != null) {
                subscribe(this, topic);
              }
            } else if (type.equals(PubSubMessageType.UNSUBSCRIBE)) {
              if (topic != null) {
                unsubscribe(this, topic);
              }
            } else if (type.equals(PubSubMessageType.PUBLISH)) {
              if (topic != null) {
                Object data = pubSubMessage.getData();
                if (data != null) {
                  publish(topic, data);
                }
                if (topic.startsWith(PubSubMessage.INTERNAL_TOPIC_PREFIX + ".")) {
                  if (internalMessageHandler != null) {
                    internalMessageHandler.onMessage(topic, data);
                  }
                }
              }
            } else if (type.equals(PubSubMessageType.SUBSCRIBE_NUM_SUBSCRIBERS)) {
              if (topic != null) {
                subscribe(this, topic + "." + PubSubMessage.NUM_SUBSCRIBERS_SUFFIX);
                sendData(this, topic + "." + PubSubMessage.NUM_SUBSCRIBERS_SUFFIX, getNumSubscribers(topic));
              }
            } else if (type.equals(PubSubMessageType.UNSUBSCRIBE_NUM_SUBSCRIBERS)) {
              if (topic != null) {
                unsubscribe(this, topic + "." + PubSubMessage.NUM_SUBSCRIBERS_SUFFIX);
              }
            } else if (type.equals(PubSubMessageType.GET_LATEST_TOPICS)) {
              synchronized (this) {
                sendData(this, "_latestTopics", latestTopics.keySet());
              }
            }
          }
        }
      } catch (Exception ex) {
        LOG.warn("Exception caught", ex);
      }
    }

    @Override
    public void onOpen(Connection connection)
    {
      LOG.debug("onOpen");
      this.connection = connection;
      this.connection.setMaxIdleTime(5 * 60 * 1000); // idle time set to five minute to clear out idle connections from taking resources
      this.connection.setMaxTextMessageSize(8 * 1024 * 1024); // allow larger text message
      messengerThread.start();
    }

    @Override
    public void onClose(int i, String string)
    {
      LOG.debug("onClose");
      disconnect(this);
      messengerThread.interrupt();
    }

    public void sendMessage(String message) throws IllegalStateException
    {
      messageQueue.add(message);
    }

    /*
     * This class exists only because Jetty 8 does not support async write for websocket
     *
     */
    private class Messenger implements Runnable
    {
      @Override
      public void run()
      {
        while (!Thread.interrupted()) {
          try {
            String message = messageQueue.take();
            // This call sendMessage() is blocking. This is why we have this messenger thread per connection so that
            // one bad connection will not affect another
            // Jetty 9 has async calls but we can't use Jetty 9 because it requires Java 7
            // When we can use Java 7, we need to upgrade to Jetty 9.
            connection.sendMessage(message);
          } catch (InterruptedException ex) {
            return;
          } catch (Exception ex) {
            LOG.error("Caught exception in websocket messenger.", ex);
            return;
          }
        }
      }

    }

  }

}

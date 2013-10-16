/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.gateway;

import com.datatorrent.api.util.JacksonObjectMapperProvider;
import com.datatorrent.api.util.PubSubMessage;
import com.datatorrent.api.util.PubSubMessage.PubSubMessageType;
import com.datatorrent.api.util.PubSubMessageCodec;

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

/**
 * <p>PubSubWebSocketServlet class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public class PubSubWebSocketServlet extends WebSocketServlet
{
  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketServlet.class);
  private static final long serialVersionUID = 1L;
  private HashMap<String, HashSet<PubSubWebSocket>> topicToSocketMap = new HashMap<String, HashSet<PubSubWebSocket>>();
  private HashMap<PubSubWebSocket, HashSet<String>> socketToTopicMap = new HashMap<PubSubWebSocket, HashSet<String>>();
  private ObjectMapper mapper = (new JacksonObjectMapperProvider()).getContext(null);
  private PubSubMessageCodec<Object> codec = new PubSubMessageCodec<Object>(mapper);
  private InternalMessageHandler internalMessageHandler = null;

  public interface InternalMessageHandler
  {
    void onMessage(String topic, Object data);

  }

  /*
   private int timeout;

   public void setTimeout(int timeout) {
   this.timeout = timeout;
   }
   */

  /*
   private int timeout;
   public void setTimeout(int timeout) {
   this.timeout = timeout;
   }
   */
  public void setInternalMessageHandler(InternalMessageHandler internalMessageHandler)
  {
    this.internalMessageHandler = internalMessageHandler;
  }

  @Override
  public WebSocket doWebSocketConnect(HttpServletRequest hsr, String protocol)
  {
    return new PubSubWebSocket();
  }

  private synchronized void subscribe(PubSubWebSocket webSocket, String topic)
  {
    HashSet<PubSubWebSocket> wsSet;
    if (!topicToSocketMap.containsKey(topic)) {
      wsSet = new HashSet<PubSubWebSocket>();
      topicToSocketMap.put(topic, wsSet);
    }
    else {
      wsSet = topicToSocketMap.get(topic);
    }
    wsSet.add(webSocket);

    HashSet<String> topicSet;
    if (!socketToTopicMap.containsKey(webSocket)) {
      topicSet = new HashSet<String>(0);
      socketToTopicMap.put(webSocket, topicSet);
    }
    else {
      topicSet = socketToTopicMap.get(webSocket);
    }
    topicSet.add(topic);
    publish(topic + ".numSubscribers", new Integer(getNumSubscribers(topic)));
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
    publish(topic + ".numSubscribers", new Integer(getNumSubscribers(topic)));
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
        publish(topic + ".numSubscribers", new Integer(getNumSubscribers(topic)));
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
    PubSubMessage<Object> pubSubMessage = new PubSubMessage<Object>();
    pubSubMessage.setType(PubSubMessageType.DATA);
    pubSubMessage.setTopic(topic);
    pubSubMessage.setData(data);
    LOG.debug("Sending data of {} to subscriber...", topic);
    webSocket.sendMessage(codec.formatMessage(pubSubMessage));
  }

  public synchronized void publish(String topic, Object data)
  {
    HashSet<PubSubWebSocket> wsSet = topicToSocketMap.get(topic);
    if (wsSet != null) {
      Iterator<PubSubWebSocket> it = wsSet.iterator();
      while (it.hasNext()) {
        PubSubWebSocket socket = it.next();
        try {
          sendData(socket, topic, data);
        }
        catch (Exception ex) {
          it.remove();
          disconnect(socket);
        }
      }
    }
  }

  private class PubSubWebSocket implements WebSocket.OnTextMessage
  {
    private Connection connection;
    private BlockingQueue<String> messageQueue = new ArrayBlockingQueue<String>(32);
    private Thread messengerThread = new Thread(new Messenger());

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
            }
            else if (type.equals(PubSubMessageType.UNSUBSCRIBE)) {
              if (topic != null) {
                unsubscribe(this, topic);
              }
            }
            else if (type.equals(PubSubMessageType.PUBLISH)) {
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
            }
            else if (type.equals(PubSubMessageType.SUBSCRIBE_NUM_SUBSCRIBERS)) {
              if (topic != null) {
                subscribe(this, topic + ".numSubscribers");
                sendData(this, topic + ".numSubscribers", new Integer(getNumSubscribers(topic)));
              }
            }
            else if (type.equals(PubSubMessageType.UNSUBSCRIBE_NUM_SUBSCRIBERS)) {
              if (topic != null) {
                unsubscribe(this, topic + ".numSubscribers");
              }
            }
          }
        }
      }
      catch (Exception ex) {
        LOG.warn("Exception caught", ex);
      }
    }

    @Override
    public void onOpen(Connection connection)
    {
      LOG.debug("onOpen");
      this.connection = connection;
      this.connection.setMaxIdleTime(60 * 60 * 1000); // idle time set to one hour to clear out idle connections from taking resources
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
            // This call sendMessage() is blocking. This is why we have this messenger thread per connection so that one bad connection will not affect another
            // Jetty 9 has async calls but we can't use Jetty 9 because it requires Java 7
            // When we can use Java 7, we need to upgrade to Jetty 9.
            connection.sendMessage(message);
          }
          catch (InterruptedException ex) {
            return;
          }
          catch (Exception ex) {
            LOG.error("Caught exception in websocket messenger.", ex);
            return;
          }
        }
      }

    }

  }

}

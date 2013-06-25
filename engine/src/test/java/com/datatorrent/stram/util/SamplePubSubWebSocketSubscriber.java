/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import com.datatorrent.api.util.PubSubWebSocketClient;
import java.net.URI;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class SamplePubSubWebSocketSubscriber implements Runnable
{
  private static final String defaultUri = "ws://localhost:9090/pubsub";
  private URI uri;
  private int messagesReceived = 0;
  private CircularFifoBuffer buffer = new CircularFifoBuffer(5);
  private String topic = "testTopic";

  public int getMessagesReceived()
  {
    return messagesReceived;
  }

  public void setUri(URI uri)
  {
    this.uri = uri;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public CircularFifoBuffer getBuffer()
  {
    return buffer;
  }

  @Override
  public void run()
  {
    try {
      if (uri == null) {
        uri = new URI(defaultUri);
      }
      PubSubWebSocketClient wsClient = new PubSubWebSocketClient()
      {
        @Override
        public void onOpen(WebSocket.Connection connection)
        {
        }

        @Override
        public void onMessage(String type, String topic, Object data)
        {
          logger.info("onMessage {}", data);
          messagesReceived++;
          buffer.add(data);
        }

        @Override
        public void onClose(int code, String message)
        {
        }

      };
      wsClient.setUri(uri);
      wsClient.openConnection(1000);
      wsClient.subscribe(topic);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }

  }

  public static void main(String[] args) throws Exception
  {
    SamplePubSubWebSocketSubscriber ss = new SamplePubSubWebSocketSubscriber();
    ss.run();
  }

  private static final Logger logger = LoggerFactory.getLogger(SamplePubSubWebSocketSubscriber.class);
}

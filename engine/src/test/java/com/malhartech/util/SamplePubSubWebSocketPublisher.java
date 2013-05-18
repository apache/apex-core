/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

import com.malhartech.api.PubSubWebSocketClient;
import com.malhartech.api.ObjectMapperString;
import static java.lang.Thread.sleep;
import java.net.URI;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class SamplePubSubWebSocketPublisher implements Runnable
{
  private static final String defaultUri = "ws://localhost:9090/pubsub";
  private URI uri;
  private ObjectMapperString payload = new ObjectMapperString("{\"hello\":\"world\"}");
  private String topic = "testTopic";

  public void setUri(URI uri)
  {
    this.uri = uri;
  }

  public void setPayload(ObjectMapperString payload)
  {
    this.payload = payload;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void run()
  {
    try {
      PubSubWebSocketClient wsClient = new PubSubWebSocketClient()
      {
        @Override
        public void onOpen(WebSocket.Connection connection)
        {
        }

        @Override
        public void onMessage(String type, String topic, Object data)
        {
        }

        @Override
        public void onClose(int code, String message)
        {
        }

      };
      if (uri == null) {
        uri = new URI(defaultUri);
      }
      wsClient.setUri(uri);
      wsClient.openConnection(1000);
      while (true) {
        wsClient.publish(topic, payload);
        sleep(1000);
      }
    }
    catch (InterruptedException ex) {
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void main(String[] args) throws Exception
  {
    SamplePubSubWebSocketPublisher sp = new SamplePubSubWebSocketPublisher();
    sp.run();
  }

  private static final Logger logger = LoggerFactory.getLogger(SamplePubSubWebSocketPublisher.class);
}

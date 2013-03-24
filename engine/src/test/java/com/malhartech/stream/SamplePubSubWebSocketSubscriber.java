/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.util.JacksonObjectMapperProvider;
import com.malhartech.util.PubSubWebSocketClient;
import java.net.URI;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class SamplePubSubWebSocketSubscriber implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(SamplePubSubWebSocketPublisher.class);
  private String channelUrl = "ws://localhost:9090/pubsub";
  private int messagesReceived = 0;
  private CircularFifoBuffer buffer = new CircularFifoBuffer(5);
  private String topic = "testTopic";
  private ObjectMapper mapper = (new JacksonObjectMapperProvider()).getContext(null);

  public int getMessagesReceived()
  {
    return messagesReceived;
  }

  public void setChannelUrl(String channelUrl)
  {
    this.channelUrl = channelUrl;
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
      URI uri = new URI(channelUrl);
      PubSubWebSocketClient wsClient = new PubSubWebSocketClient()
      {
        @Override
        public void onOpen(WebSocket.Connection connection)
        {
        }

        @Override
        public void onMessage(String type, String topic, Object data)
        {
          LOG.info("onMessage {}", data);
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

}

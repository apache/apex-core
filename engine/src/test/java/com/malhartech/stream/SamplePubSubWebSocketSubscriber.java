/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.util.JacksonObjectMapperProvider;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocket.Connection;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class SamplePubSubWebSocketSubscriber implements Runnable
{
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
      WebSocketClientFactory factory = new WebSocketClientFactory();
      factory.setBufferSize(8192);
      factory.start();

      WebSocketClient client = factory.newWebSocketClient();

      URI uri = new URI(channelUrl);

      WebSocket.Connection connection = client.open(uri, new WebSocket.OnTextMessage()
      {
        @Override
        public void onMessage(String string)
        {
          System.out.println("onMessage " + string);
          messagesReceived++;
          buffer.add(string);
        }

        @Override
        public void onOpen(Connection cnctn)
        {
          System.out.println("onOpen");
        }

        @Override
        public void onClose(int i, String string)
        {
          System.out.println("onClose " + i + "," + string);
        }

      }).get(5, TimeUnit.SECONDS);
      HashMap<String,Object> map = new HashMap<String,Object>();
      map.put("type", "subscribe");
      map.put("topic", topic);
      connection.sendMessage(mapper.writeValueAsString(map));
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

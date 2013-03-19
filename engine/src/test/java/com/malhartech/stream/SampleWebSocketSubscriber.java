/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocket.Connection;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class SampleWebSocketSubscriber implements Runnable
{
  private String channelUrl = "ws://localhost:9090/channel/testChannel";
  private int messagesReceived = 0;
  private CircularFifoBuffer buffer = new CircularFifoBuffer(5);

  public int getMessagesReceived()
  {
    return messagesReceived;
  }

  public void setChannelUrl(String channelUrl)
  {
    this.channelUrl = channelUrl;
  }

  public CircularFifoBuffer getBuffer() {
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

      client.open(uri, new WebSocket.OnTextMessage()
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
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void main(String[] args) throws Exception
  {
    SampleWebSocketSubscriber ss = new SampleWebSocketSubscriber();
    ss.run();
  }

}

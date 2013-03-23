/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.util.JacksonObjectMapperProvider;
import com.malhartech.util.ObjectMapperString;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocket.Connection;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class SamplePubSubWebSocketPublisher implements Runnable
{
  private String channelUrl = "http://localhost:9090/pubsub";
  private static final Logger LOG = LoggerFactory.getLogger(SamplePubSubWebSocketPublisher.class);
  private ObjectMapperString payload = new ObjectMapperString("{\"hello\":\"world\"}");
  private String topic = "testTopic";
  private ObjectMapper mapper = (new JacksonObjectMapperProvider()).getContext(null);

  public void setChannelUrl(String channelUrl)
  {
    this.channelUrl = channelUrl;
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
  public void run()
  {
    try {
      WebSocketClientFactory factory = new WebSocketClientFactory();
      factory.start();
      WebSocketClient client = factory.newWebSocketClient();
      WebSocket.Connection connection = client.open(new URI(channelUrl), new WebSocket.OnTextMessage() {

        @Override
        public void onMessage(String string)
        {
          LOG.info("onMessage {}", string);
        }

        @Override
        public void onOpen(Connection cnctn)
        {
          LOG.info("onOpen {}", cnctn);
        }

        @Override
        public void onClose(int i, String string)
        {
          LOG.info("onClose {} {}", i, string);
        }
      }).get(5, TimeUnit.SECONDS);

      HashMap<String,Object> map = new HashMap<String,Object>();
      map.put("type", "publish");
      map.put("topic", topic);
      map.put("data", payload);

      String message = mapper.writeValueAsString(map);
      while (true) {
        connection.sendMessage(message);
        Thread.sleep(1000);
      }
    } catch (InterruptedException ex) {
      return;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void main(String[] args) throws Exception
  {
    SamplePubSubWebSocketPublisher sp = new SamplePubSubWebSocketPublisher();
    sp.run();
  }

}

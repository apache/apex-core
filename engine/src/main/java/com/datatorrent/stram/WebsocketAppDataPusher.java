/*
 *  Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.common.util.PubSubWebSocketClient;
import com.datatorrent.stram.api.AppDataPusher;
import java.io.IOException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class WebsocketAppDataPusher implements AppDataPusher
{
  private final String topic;
  private long resendSchemaInterval = 10000; // 10 seconds
  protected PubSubWebSocketClient client;


  public WebsocketAppDataPusher(PubSubWebSocketClient wsClient, String topic)
  {
    client = wsClient;
    this.topic = topic;
  }

  public void setResendSchemaInterval(long resendSchemaInterval)
  {
    this.resendSchemaInterval = resendSchemaInterval;
  }

  @Override
  public void push(JSONObject msg) throws IOException
  {
    client.publish(topic, msg);
  }

  @Override
  public long getResendSchemaInterval()
  {
    return resendSchemaInterval;
  }

  private static final Logger LOG = LoggerFactory.getLogger(WebsocketAppDataPusher.class);

}

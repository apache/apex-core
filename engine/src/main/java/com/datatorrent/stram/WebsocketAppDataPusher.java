/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram;

import com.datatorrent.common.util.PubSubWebSocketClient;
import com.datatorrent.stram.api.AppDataPusher;
import java.io.IOException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>WebsocketAppDataPusher class.</p>
 *
 * @since 3.0.0
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

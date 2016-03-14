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
package com.datatorrent.stram;

import java.io.IOException;
import java.io.Serializable;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.stram.util.PubSubWebSocketClient;

/**
 * <p>PubSubWebSocketMetricTransport class.</p>
 *
 * @since 3.0.0
 */
public class PubSubWebSocketMetricTransport implements AutoMetric.Transport, Serializable
{
  private final String topic;
  private final long schemaResendInterval;
  protected PubSubWebSocketClient client;

  public PubSubWebSocketMetricTransport(PubSubWebSocketClient wsClient, String topic, long schemaResendInterval)
  {
    client = wsClient;
    this.topic = topic;
    this.schemaResendInterval = schemaResendInterval;
  }

  @Override
  public void push(String msg) throws IOException
  {
    try {
      client.publish(topic, new JSONObject(msg));
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public long getSchemaResendInterval()
  {
    return schemaResendInterval;
  }

  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketMetricTransport.class);
  private static final long serialVersionUID = 201512301008L;
}

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
package com.datatorrent.common.metric;

import java.io.IOException;
import java.io.Serializable;

import com.datatorrent.api.AutoMetric;

/**
 * AutoMetricBuiltinTransport. This will be replaced by the internal websocket pubsub transport
 * provided here: {@link com.datatorrent.stram.PubSubWebSocketMetricTransport}.
 *
 * @since 3.3.0
 */
public class AutoMetricBuiltInTransport implements AutoMetric.Transport, Serializable
{
  private final String topic;
  private final long schemaResendInterval;
  private static final long DEFAULT_SCHEMA_RESEND_INTERVAL = 10000;

  public AutoMetricBuiltInTransport(String topic)
  {
    this.topic = topic;
    this.schemaResendInterval = DEFAULT_SCHEMA_RESEND_INTERVAL;
  }

  public AutoMetricBuiltInTransport(String topic, long schemaResendInterval)
  {
    this.topic = topic;
    this.schemaResendInterval = schemaResendInterval;
  }

  @Override
  public String toString()
  {
    return this.topic;
  }

  @Override
  public void push(String jsonData) throws IOException
  {
    throw new UnsupportedOperationException("This class is a placeholder and is supposed to replaced by internal " +
        "implementation.");
  }

  @Override
  public long getSchemaResendInterval()
  {
    return schemaResendInterval;
  }

  public String getTopic()
  {
    return topic;
  }

  private static final long serialVersionUID = 201512301009L;
}

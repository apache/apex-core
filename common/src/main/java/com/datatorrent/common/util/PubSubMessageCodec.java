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
package com.datatorrent.common.util;

import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;

import com.datatorrent.common.util.PubSubMessage.PubSubMessageType;

/**
 * <p>PubSubMessageCodec class.</p>
 *
 * @param <T>
 * @since 0.3.5
 */
public class PubSubMessageCodec<T>
{

  private final ObjectMapper mapper;

  public PubSubMessageCodec(ObjectMapper mapper)
  {
    this.mapper = mapper;
  }

  /**
   * <p>constructPublishMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param data
   * @param codec
   * @return publish message.
   * @throws IOException
   */
  public static <T> String constructPublishMessage(String topic, T data, PubSubMessageCodec<T> codec) throws IOException
  {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<>();
    pubSubMessage.setType(PubSubMessageType.PUBLISH);
    pubSubMessage.setTopic(topic);
    pubSubMessage.setData(data);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>constructSubscribeMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return subscribe message.
   * @throws IOException
   */
  public static <T> String constructSubscribeMessage(String topic, PubSubMessageCodec<T> codec) throws IOException
  {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<>();
    pubSubMessage.setType(PubSubMessageType.SUBSCRIBE);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>constructUnsubscribeMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return un-subscribe message.
   * @throws IOException
   */
  public static <T> String constructUnsubscribeMessage(String topic, PubSubMessageCodec<T> codec) throws IOException
  {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<>();
    pubSubMessage.setType(PubSubMessageType.UNSUBSCRIBE);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>constructSubscribeNumSubscribersMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return subscribe num subscriber message.
   * @throws IOException
   */
  public static <T> String constructSubscribeNumSubscribersMessage(String topic, PubSubMessageCodec<T> codec) throws IOException
  {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<>();
    pubSubMessage.setType(PubSubMessageType.SUBSCRIBE_NUM_SUBSCRIBERS);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  /**
   * <p>constructUnsubscribeNumSubscribersMessage.</p>
   *
   * @param <T>
   * @param topic
   * @param codec
   * @return un-subscribe num subscribers message.
   * @throws IOException
   */
  public static <T> String constructUnsubscribeNumSubscribersMessage(String topic, PubSubMessageCodec<T> codec) throws IOException
  {
    PubSubMessage<T> pubSubMessage = new PubSubMessage<>();
    pubSubMessage.setType(PubSubMessageType.UNSUBSCRIBE_NUM_SUBSCRIBERS);
    pubSubMessage.setTopic(topic);

    return codec.formatMessage(pubSubMessage);
  }

  public String formatMessage(PubSubMessage<T> pubSubMessage) throws IOException
  {
    HashMap<String, Object> map = new HashMap<>();
    map.put(PubSubMessage.TYPE_KEY, pubSubMessage.getType().getIdentifier());
    map.put(PubSubMessage.TOPIC_KEY, pubSubMessage.getTopic());
    T data = pubSubMessage.getData();
    if (data != null) {
      map.put(PubSubMessage.DATA_KEY, data);
    }

    return mapper.writeValueAsString(map);
  }

  /**
   *
   * @param message
   * @return a {@link PubSubMessage} message.
   * @throws IOException
   */
  @SuppressWarnings({"unchecked"})
  public PubSubMessage<T> parseMessage(String message) throws IOException
  {
    HashMap<String, Object> map = mapper.readValue(message, HashMap.class);
    PubSubMessage<T> pubSubMessage = new PubSubMessage<>();
    pubSubMessage.setType(PubSubMessageType.getPubSubMessageType((String)map.get(PubSubMessage.TYPE_KEY)));
    pubSubMessage.setTopic((String)map.get(PubSubMessage.TOPIC_KEY));
    pubSubMessage.setData((T)map.get(PubSubMessage.DATA_KEY));
    return pubSubMessage;
  }
}

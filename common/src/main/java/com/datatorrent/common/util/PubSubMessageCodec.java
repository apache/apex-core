/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;

import com.datatorrent.lib.util.PubSubMessage.PubSubMessageType;

/**
 * <p>PubSubMessageCodec class.</p>
 *
 * @param <T>
 * @since 0.3.5
 */
public class PubSubMessageCodec<T>
{

  private final ObjectMapper mapper;

  public PubSubMessageCodec(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public String formatMessage(PubSubMessage<T> pubSubMessage) throws IOException {
    HashMap<String, Object> map = new HashMap<String, Object>();
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
   * @return
   * @throws IOException
   */
  @SuppressWarnings({"unchecked"})
  public PubSubMessage<T> parseMessage(String message) throws IOException {
    HashMap<String, Object> map = mapper.readValue(message, HashMap.class);
    PubSubMessage<T> pubSubMessage = new PubSubMessage<T>();
    pubSubMessage.setType(PubSubMessageType.getPubSubMessageType((String)map.get(PubSubMessage.TYPE_KEY)));
    pubSubMessage.setTopic((String)map.get(PubSubMessage.TOPIC_KEY));
    pubSubMessage.setData((T)map.get(PubSubMessage.DATA_KEY));
    return pubSubMessage;
  }
}

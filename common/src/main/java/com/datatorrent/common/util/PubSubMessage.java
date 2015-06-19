/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

/**
 * <p> PubSubMessage </p>
 *
 * @param <T>
 * @since 0.3.5
 */
public class PubSubMessage<T>
{
  public static final String TYPE_KEY = "type";
  public static final String TOPIC_KEY = "topic";
  public static final String DATA_KEY = "data";
  public static final String INTERNAL_TOPIC_PREFIX = "_internal";
  public static final String NUM_SUBSCRIBERS_SUFFIX = "numSubscribers";

  public static enum PubSubMessageType
  {
    PUBLISH("publish"),
    SUBSCRIBE("subscribe"),
    UNSUBSCRIBE("unsubscribe"),
    SUBSCRIBE_NUM_SUBSCRIBERS("subscribeNumSubscribers"),
    UNSUBSCRIBE_NUM_SUBSCRIBERS("unsubscribeNumSubscribers"),
    DATA("data"),
    GET_LATEST_TOPICS("getLatestTopics");
    private final String identifier;

    PubSubMessageType(String identifier)
    {
      this.identifier = identifier;
    }

    public String getIdentifier()
    {
      return identifier;
    }

    public static PubSubMessageType getPubSubMessageType(String identifier) {
      PubSubMessageType pubSubMessageType = null;
      for (PubSubMessageType value : PubSubMessageType.values()) {
        if (value.getIdentifier().equals(identifier)) {
          pubSubMessageType = value;
          break;
        }
      }
      return pubSubMessageType;
    }

  }

  private PubSubMessageType type;
  private String topic;
  private T data;

  public PubSubMessageType getType()
  {
    return type;
  }

  public void setType(PubSubMessageType type)
  {
    this.type = type;
  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public T getData()
  {
    return data;
  }

  public void setData(T data)
  {
    this.data = data;
  }

}

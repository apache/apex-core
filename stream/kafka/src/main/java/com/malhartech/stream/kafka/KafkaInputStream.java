/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream.kafka;

import com.malhartech.dag.StreamConfiguration;
import com.malhartech.stream.AbstractInputAdapter;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

/**
 * This is a Input Adapter Code from Kafka message bus. Make sure that you have
 * Kafka installed (look at pom.xml for instructions)
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class KafkaInputStream
  extends AbstractInputAdapter
  implements Runnable
{
  private ConsumerConnector consumer;
  private String topic;

  @Override
  public void setup(StreamConfiguration config)
  {
    Properties props = new Properties();
    String interesting[] = {
      "zk.connect",
      "zk.connectiontimeout.ms",
      "groupid",
      "topic"
    };

    for (String s : interesting) {
      if (config.get(s) != null) {
        props.put(s, config.get(s));
      }
    }

    topic = props.containsKey("topic") ? props.getProperty("topic") : "";
    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
  }

  @Override
  public void run()
  {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<Message> stream = consumerMap.get(topic).get(0);
    ConsumerIterator<Message> it = stream.iterator();
    while (it.hasNext()) {
      emit(getObject(it.next().message()));
    }
  }

  @Override
  public void teardown()
  {
    consumer.shutdown();
    consumer = null;
    topic = null;
  }

  public Object getObject(Object message)
  {
    /*
     * get the object from message
     */
    if (message instanceof Message) {
      ByteBuffer buffer = ((Message) message).payload();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);

      return context.getSerDe().fromByteArray(bytes);
    }

    return null;
  }

  @Override
  public void activate()
  {
    new Thread(this).start();
  }
}

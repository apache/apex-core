/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.PartitionedData;
import com.malhartech.bufferserver.Buffer.SimpleData;
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
public class InputKafkaStream implements Stream, Runnable
{

  private StreamContext context;
  private ConsumerConnector consumer;
  private String topic;

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
    
    topic = props.containsKey("topic")? props.getProperty("topic"): "";
    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
  }

  public void setContext(StreamContext context)
  {
    this.context = context;
    new Thread(this).start();
  }

  public void run()
  {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<Message> stream = consumerMap.get(topic).get(0);
    ConsumerIterator<Message> it = stream.iterator();
    while (it.hasNext()) {
      Message message = it.next().message();
      Tuple t = getTuple(message);
      context.getSink().doSomething(t);
    }
  }

  public Tuple getTuple(Message message)
  {
    /*
     * get the object from message
     */
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);


    SerDe serde = context.getSerDe();
    Object o = serde.fromByteArray(bytes);
    byte[] partition = serde.getPartition(o);

    Data.Builder db = Data.newBuilder();
    db.setWindowId(0); // set it to appropriate window Id
    if (partition == null) {
      SimpleData.Builder sdb = SimpleData.newBuilder();
      sdb.setData(ByteString.EMPTY);

      /*
       * we dont care about byte array
       */
      db.setType(Data.DataType.SIMPLE_DATA);
      db.setSimpledata(sdb);
    }
    else {
      PartitionedData.Builder pdb = PartitionedData.newBuilder();
      pdb.setPartition(ByteString.copyFrom(partition));

      /*
       * we dont care about byte array
       */
      pdb.setData(ByteString.EMPTY);
      db.setType(Data.DataType.PARTITIONED_DATA);
      db.setPartitioneddata(pdb);
    }

    Tuple t = new Tuple(o);
    t.setContext(context);
    t.setData(db.build());
    return t;
  }

  public void teardown(StreamConfiguration config)
  {
    consumer.shutdown();
    consumer = null;
    topic = null;
  }
}

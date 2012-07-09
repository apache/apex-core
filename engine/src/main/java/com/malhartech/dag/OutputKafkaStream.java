/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import kafka.javaapi.producer.Producer;


/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class OutputKafkaStream
        implements Stream, Sink
{

  public void setup(StreamConfiguration config)
  {
    Producer producer;
    
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public void setContext(StreamContext context)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public void teardown(StreamConfiguration config)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public void doSomething(Tuple t)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}

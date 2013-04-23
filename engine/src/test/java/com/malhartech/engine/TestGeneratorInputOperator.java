/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.engine;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestGeneratorInputOperator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(TestGeneratorInputOperator.class);
  public static final String OUTPUT_PORT = "outputPort";
  public static final String KEY_MAX_TUPLES = "maxTuples";
  private String myConfigProperty;
  private int maxTuples = -1;
  private int generatedTuples = 0;
  private int remainingSleepTime;
  private int emitInterval = 1000;
  private final int spinMillis = 50;
  private final ConcurrentLinkedQueue<String> externallyAddedTuples = new ConcurrentLinkedQueue<String>();
  @OutputPortFieldAnnotation(name = "outputPort", optional=false)
  final public transient DefaultOutputPort<Object> outport = new DefaultOutputPort<Object>(this);

  public int getMaxTuples()
  {
    return maxTuples;
  }

  public void setEmitInterval(int emitInterval)
  {
    this.emitInterval = emitInterval;
  }

  public void setMaxTuples(int maxNumbers)
  {
    LOG.info("setting max tuples to {}", maxNumbers);
    this.maxTuples = maxNumbers;
  }

  public String getMyConfigProperty()
  {
    return myConfigProperty;
  }

  public void setMyConfigProperty(String myConfigProperty)
  {
    this.myConfigProperty = myConfigProperty;
  }

  @Override
  public void emitTuples()
  {
    Object tuple;
    while ((tuple = this.externallyAddedTuples.poll()) != null) {
      outport.emit(tuple);
    }

    if (remainingSleepTime > 0) {
      try {
        Thread.sleep(spinMillis);
        remainingSleepTime -= spinMillis;
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    else if (maxTuples != 0) {
      generatedTuples++;
      LOG.info("sending tuple " + generatedTuples);
      outport.emit(String.valueOf(generatedTuples));
      if (maxTuples > 0 && maxTuples <= generatedTuples) {
        throw new RuntimeException(new InterruptedException("done emitting all."));
      }
      remainingSleepTime = emitInterval;
    }
    else {
      remainingSleepTime = emitInterval;
    }
  }

  /**
   * Manually add a tuple to emit.
   */
  public void addTuple(String s)
  {
    externallyAddedTuples.add(s);
  }
}

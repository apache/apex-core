/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.dag;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.InputModule;
import com.malhartech.api.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ModuleAnnotation(
    ports = {
  @PortAnnotation(name = TestGeneratorInputModule.OUTPUT_PORT, type = PortType.OUTPUT)
})
public class TestGeneratorInputModule extends InputModule
{
  private static final Logger LOG = LoggerFactory.getLogger(TestGeneratorInputModule.class);
  public static final String OUTPUT_PORT = "outputPort";
  public static final String KEY_MAX_TUPLES = "maxTuples";
  private String myConfigProperty;
  private int maxTuples = -1;
  private int generatedTuples = 0;
  private boolean outputConnected = false;
  private int remainingSleepTime;
  private final ConcurrentLinkedQueue<String> externallyAddedTuples = new ConcurrentLinkedQueue<String>();

  public int getMaxTuples()
  {
    return maxTuples;
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
  public void connected(String id, Sink dagpart)
  {
    if (OUTPUT_PORT.equals(id)) {
      this.outputConnected = true;
    }
  }

  @Override
  public void process(Object payload)
  {
    Object tuple;
    while ((tuple = this.externallyAddedTuples.poll()) != null) {
      emit(OUTPUT_PORT, tuple);
    }

    if (remainingSleepTime > 0) {
      try {
        Thread.sleep(spinMillis);
        remainingSleepTime -= spinMillis;
      }
      catch (InterruptedException ie) {
      }
    }
    else if (outputConnected && maxTuples != 0) {
      generatedTuples++;
      LOG.info("sending tuple " + generatedTuples);
      emit(OUTPUT_PORT, String.valueOf(generatedTuples));
      if (maxTuples > 0 && maxTuples < generatedTuples) {
        deactivate();
      }
      remainingSleepTime = 1000;
    } else {
      remainingSleepTime = 1000;
    }
  }

  /**
   * Manually add a tuple to emit.
   */
  public void addTuple(String s) {
    externallyAddedTuples.add(s);
  }


}

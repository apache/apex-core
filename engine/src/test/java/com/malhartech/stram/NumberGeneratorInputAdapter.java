/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;

@NodeAnnotation(
    ports = {
        @PortAnnotation(name = NumberGeneratorInputAdapter.OUTPUT_PORT, type = PortType.OUTPUT)
    }
)
public class NumberGeneratorInputAdapter extends AbstractNode
  implements Runnable
{
  private static Logger LOG = LoggerFactory.getLogger(NumberGeneratorInputAdapter.class);
  public static final String OUTPUT_PORT = "outputPort";
  
  private volatile boolean shutdown = false;
  private String myConfigProperty;
  private int maxTuples = -1;
  private int generatedNumbers = 0;

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
  public void process(Object payload) {
    
  }

  @Override
  public void handleIdleTimeout() {
    if (hasFinished()) {
      deactivate();
    }
  }

  private boolean hasFinished() {
    if (maxTuples > 0 && maxTuples < generatedNumbers) {
      return true;
    }
    return false;
  }
  
  @Override
  public void run()
  {
    while (!shutdown && !hasFinished()) {
      LOG.debug("sending tuple");
      emit(String.valueOf(generatedNumbers++));
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        LOG.error("Unexpected error in run.", e);
      }
    }
    LOG.info("Finished generating tuples");
  }

  @Override
  public void teardown()
  {
    shutdown = true;
  }

  @Override // this was activate
  public void setup(NodeConfiguration config) {
    Thread t = new Thread(this);
    t.start();
  }

}

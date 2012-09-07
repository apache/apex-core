/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractInputNode;
import com.malhartech.dag.EndStreamTuple;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NodeAnnotation(
    ports = {
        @PortAnnotation(name = NumberGeneratorInputAdapter.OUTPUT_PORT, type = PortType.OUTPUT)
    }
)
public class NumberGeneratorInputAdapter extends AbstractInputNode
{
  private static Logger LOG = LoggerFactory.getLogger(NumberGeneratorInputAdapter.class);
  public static final String OUTPUT_PORT = "outputPort";

  private volatile boolean shutdown = false;
  private String myConfigProperty;
  private int maxTuples = -1;
  private int generatedNumbers = 0;
  private boolean outputConnected = false;

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
  public void connected(String id, Sink dagpart) {
    if (OUTPUT_PORT.equals(id)) {
      this.outputConnected = true;
    }
  }

  @SuppressWarnings("SleepWhileInLoop")
  private void run()
  {
    while (!shutdown) {
      if (outputConnected) {
        LOG.debug("sending tuple");
        generatedNumbers++;
        emit(OUTPUT_PORT, String.valueOf(generatedNumbers));
        if (maxTuples > 0 && maxTuples < generatedNumbers) {
          emit(OUTPUT_PORT, new EndStreamTuple());
          break;
        }
      }
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
  public void activate(NodeContext context) {
    super.activate(context);
    run();
  }

  @Override
  public void deactivate() {
    this.shutdown = true;
    super.deactivate();
  }

  @Override
  public void beginWindow() {
  }

  @Override
  public void endWindow() {
  }
}

/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractInputModule;
import com.malhartech.dag.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ModuleAnnotation(
    ports = {
  @PortAnnotation(name = TestGeneratorInputModule.OUTPUT_PORT, type = PortType.OUTPUT)
})
public class TestGeneratorInputModule extends AbstractInputModule
{
  private static final Logger LOG = LoggerFactory.getLogger(TestGeneratorInputModule.class);
  public static final String OUTPUT_PORT = "outputPort";
  public static final String KEY_MAX_TUPLES = "maxTuples";

  private volatile boolean shutdown = false; // how do we handle this now that deactivate is not overridable.
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
  public void connected(String id, Sink dagpart)
  {
    if (OUTPUT_PORT.equals(id)) {
      this.outputConnected = true;
    }
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void run()
  {
    while (!shutdown) {
      if (outputConnected) {
        generatedNumbers++;
        LOG.info("sending tuple " + generatedNumbers);
        emit(OUTPUT_PORT, String.valueOf(generatedNumbers));
        if (maxTuples > 0 && maxTuples < generatedNumbers) {
          break;
        }
      }
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        LOG.info("Exiting generator loop.", e);
        break;
      }
    }
    LOG.info("Finished generating tuples");
  }

  @Override
  public void teardown() {
    shutdown = true;
    super.teardown();
  }

}

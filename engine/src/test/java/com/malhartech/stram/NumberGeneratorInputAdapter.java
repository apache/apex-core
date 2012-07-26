/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.StreamConfiguration;
import com.malhartech.stream.AbstractObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NumberGeneratorInputAdapter extends AbstractObjectInputStream
    implements Runnable {
  private static Logger LOG = LoggerFactory
      .getLogger(NumberGeneratorInputAdapter.class);
  private volatile boolean shutdown = false;
  private String myConfigProperty;
  private int maxTuples = -1;
  private int generatedNumbers = 0;

  public int getMaxTuples() {
    return maxTuples;
  }

  public void setMaxTuples(int maxNumbers) {
    LOG.info("setting max tuples to {}", maxNumbers);
    this.maxTuples = maxNumbers;
  }

  public String getMyConfigProperty() {
    return myConfigProperty;
  }

  public void setMyConfigProperty(String myConfigProperty) {
    this.myConfigProperty = myConfigProperty;
  }

  @Override
  public void setup(StreamConfiguration config) {
  }

  @Override
  public boolean hasFinished() {
    return maxTuples > 0 && maxTuples > generatedNumbers;
  }

  public void run() {
    while (!shutdown && !hasFinished()) {
      LOG.info("sending tuple to: " + context);
      emit(String.valueOf(generatedNumbers++));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error("Unexpected error in run.", e);
      }
    }
    LOG.info("Finished generating tuples");
  }

  @Override
  public void teardown() {
    shutdown = true;
  }

  @Override
  public Object getObject(Object object) {
    return null;
  }

  public void activate()
  {
    Thread t = new Thread(this);
    t.start();
  }

}

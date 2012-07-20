/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.stream.AbstractObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NumberGeneratorInputAdapter extends AbstractObjectInputStream
    implements Runnable {
  private static Logger LOG = LoggerFactory
      .getLogger(NumberGeneratorInputAdapter.class);
  private volatile boolean shutdown = false;
  private String myConfigProperty;
  
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
  public void setContext(StreamContext context) {
    super.setContext(context);
    this.context = context;
    Thread t = new Thread(this);
    t.start();
  }

  public void run() {
    int i = 0;
    while (!shutdown) {
      LOG.info("sending tuple to: " + context);
      sendTuple(String.valueOf(i++));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error("Unexpected error in run.", e);
      }
    }
  }

  @Override
  public void teardown() {
    shutdown = true;
  }

  @Override
  public Object getObject(Object object) {
    return null;
  }

  public StreamContext getContext()
  {
    return context;
  }

}

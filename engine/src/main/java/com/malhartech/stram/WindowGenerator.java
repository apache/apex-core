/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.InputAdapter;
import java.util.Collection;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WindowGenerator {
  
  final long startMillis;
  final long intervalMillis;
  long currentWindowMillis = -1;
  final Collection<InputAdapter> inputAdapters;
  ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1);
  
  public WindowGenerator(Collection<InputAdapter> inputs, long startMillis, long intervalMillis) {
    this.inputAdapters = inputs;
    this.startMillis = startMillis;
    this.intervalMillis = intervalMillis;
    this.currentWindowMillis = startMillis;
  }
  
  protected void nextWindow() {
    for (InputAdapter ia : inputAdapters) {
      if (currentWindowMillis != startMillis) {
        ia.endWindow(currentWindowMillis - intervalMillis);
      }
      ia.beginWindow(currentWindowMillis);
    }
    currentWindowMillis += intervalMillis;
  }

  public void start() {
    long currentTms = System.currentTimeMillis();
    // generate begin/end for elapsed windows
    while (currentWindowMillis < currentTms) {
      nextWindow();
    }
    // schedule future windows
    Runnable r = new Runnable() {
      @Override
      public void run() {
        nextWindow();
      }
    };
    stpe.scheduleAtFixedRate(r, currentWindowMillis - currentTms, intervalMillis, TimeUnit.MILLISECONDS);
  }
  
  public void stop() {
    stpe.shutdown();
  }
  
}
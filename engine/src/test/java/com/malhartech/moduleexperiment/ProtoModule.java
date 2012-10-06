/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

/**
 *
 */
public interface ProtoModule {

  public static interface InputPort<T extends Object> {
    void process(T payload);
  }

  void beginWindow();
  void endWindow();

  public void processPort1(String s);

}

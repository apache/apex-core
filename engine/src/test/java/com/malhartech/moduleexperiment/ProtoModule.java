/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import com.malhartech.dag.Sink;

/**
 *
 */
public interface ProtoModule {

  /**
   * Input ports are declared as annotated factory methods and return a port
   * object that the execution engine will call to pass the tuples.
   *
   * @param <T>
   */
  public static interface InputPort<T extends Object> {
    public void process(T payload);
  }

  /**
   * Output ports are declared as annotated typed fields by the module.
   * The module processing logic simply calls emit on the port object.
   * Output ports also define how output from replicated operators is merged.
   * @param <T>
   */
  public static class OutputPort<T extends Object> {
    private transient Sink sink;

    final public void emit(T payload) {
      sink.process(payload);
    }

    /**
     * Internally called by execution engine to inject sink.
     * @param s
     */
    final void setSink(Sink s) {
      this.sink = s;
    }

    /**
     * Opportunity for user code to check whether the port is connected, if optional.
     */
    public boolean isConnected() {
      return sink != null;
    }

    /**
     * Module developer can override for merge functionality
     * @param o
     */
    public void merge(T o) {
    }

  }


  void beginWindow();
  void endWindow();

  public void processGeneric(Object payload);

}

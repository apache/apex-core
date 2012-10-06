/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

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
   * Output ports are declared as annotated fields by the module and injected by the execution engine.
   * The module developer simply calls emit on the typed port object.
   *
   * @param <T>
   */
  public static interface OutputPort<T extends Object> {
    public void emit(T payload);
  }


  void beginWindow();
  void endWindow();

  public void processGeneric(Object payload);

}

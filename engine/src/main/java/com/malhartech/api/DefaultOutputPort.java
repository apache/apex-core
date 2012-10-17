/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.api;

/**
 * Output ports are declared as annotated typed fields by the module. The
 * module processing logic simply calls emit on the port object. Output ports
 * also define how output from replicated operators is merged.
 *
 * @param <T>
 */
public class DefaultOutputPort<T>  implements Operator.OutputPort<T> {
  private final Operator module;
  private transient Sink<T> sink;

  public DefaultOutputPort(Operator module) {
    this.module = module;
  }

  @Override
  final public Operator getOperator() {
    return module;
  }

  final public void emit(T payload) {
    sink.process(payload);
  }

  /**
   * Called by execution engine to inject sink at deployment time.
   *
   * @param s
   */
  @Override
  final public void setSink(Sink<T> s) {
    this.sink = s;
  }

  /**
   * Opportunity for user code to check whether the port is connected, if
   * optional.
   */
  public boolean isConnected() {
    return sink != null;
  }

  /**
   * Module developer can override for merge functionality
   *
   * @param o
   */
  public void merge(T o) {
    sink.process(o);
  }

}
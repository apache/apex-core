/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.api;

import com.malhartech.dag.OperatorContext;

/**
 * Output ports are declared as annotated typed fields by the operator. The
 * operator processing logic simply calls emit on the port object. Output ports
 * also define how output from replicated operators is merged.
 *
 * @param <T>
 */
public class DefaultOutputPort<T>  implements Operator.OutputPort<T> {
  private final Operator operator;
  private transient Sink<T> sink;

  public DefaultOutputPort(Operator operator) {
    this.operator = operator;
  }

  @Override
  final public Operator getOperator() {
    return operator;
  }

  final public void emit(T tuple) {
    sink.process(tuple);
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
   * Module developer can override for getUnifier functionality
   *
   * @param o
   */
  @Override
  public Operator getUnifier() {
//    return new BaseOperator() {
//    };
//    sink.process(tuple);
    return null;
  }

}
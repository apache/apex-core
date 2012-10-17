/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.api;

import com.malhartech.api.Operator.InputPort;

/**
 * Default abstract implementation for input ports. A module would typically
 * create a derived inner class that defines process. This class is designed for
 * use with a transient field, i.e. not to be serialized with the module state.
 */
public abstract class DefaultInputPort<T> implements InputPort<T>, Sink<T> {
  final private Operator module;
  protected boolean connected = false;

  public DefaultInputPort(Operator module) {
    this.module = module;
  }

  @Override
  final public Operator getOperator() {
    return module;
  }

  @Override
  public Sink<T> getSink() {
    return this;
  }

  @Override
  public void setConnected(boolean connected) {
    this.connected = connected;
  }

  /**
   * Processing logic to be implemented in derived class.
   */
  @Override
  public abstract void process(T payload);
}

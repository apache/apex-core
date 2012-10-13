/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.Sink;

/**
 * The base class for all module implementations
 */
public abstract class ProtoModule {

  /**
   * A user friendly name that is available to identify the module in the system.
   */
  private String name;

  /**
   * Input ports are declared as annotated fields.
   * The execution engine will call the port object to pass the tuples.
   * Since the interface is implemented by the user, the type parameters are available at runtime for validation.
   *
   * @param <T>
   */
  public abstract static class InputPort<T extends Object> {
    final ProtoModule module;

    public InputPort(ProtoModule module) {
      this.module = module;
    }

    /**
     * Defines the processing logic.
     * @param payload
     */
    abstract public void process(T payload);
  }

  /**
   * Output ports are declared as annotated typed fields by the module.
   * The module processing logic simply calls emit on the port object.
   * Output ports also define how output from replicated operators is merged.
   * @param <T>
   */
  public static class OutputPort<T extends Object> {
    final ProtoModule module;
    private transient Sink sink;

    public OutputPort(ProtoModule module) {
      this.module = module;
    }


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

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void beginWindow()
  {
  }

  public void endWindow()
  {
  }

  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
  }

  public void processGeneric(Object payload) {};

}

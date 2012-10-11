/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import java.util.HashMap;
import java.util.Map;


/**
 *
 */
public class MyProtoModule<T extends Object> implements ProtoModule {

  @ProtoOutputPortFieldAnnotation(name="outport1")
  private final OutputPort<Map<String, T>> outport1 = new OutputPort<Map<String,T>>();

  @ProtoOutputPortFieldAnnotation(name="outport2")
  private final OutputPort<byte[]> outport2 = new OutputPort<byte[]>();

  @Override
  public void beginWindow() {
  }

  @Override
  public void endWindow() {
  }

  @ProtoInputPortProcessAnnotation(name="methodAnnotatedPort1")
  public void processPort1(String s) {

  }

  @Override
  public void processGeneric(Object payload) {
  }

  /**
   * Example for (runtime) typed input port.
   * The type information is retained at runtime and can be used for validation by the framework.
   */
  @ProtoInputPortGetAnnotation(name="port1")
  public InputPort<String> inport1 = new InputPort<String>() {
    @Override
    final public void process(String payload) {
    }
  };

  /**
   * Untyped input port implemented using anonymous class
   * The port is untyped because it is using the enclosing classes type parameter.
   */
  @ProtoInputPortGetAnnotation(name="port2")
  public InputPort<T> inport2 = new InputPort<T>() {
    @Override
    final public void process(T payload) {
      if (outport1.isConnected()) {
          HashMap<String, T> m = new HashMap<String, T>();
          m.put("payload", payload);
          outport1.emit(m);
      }
    }
  };

}

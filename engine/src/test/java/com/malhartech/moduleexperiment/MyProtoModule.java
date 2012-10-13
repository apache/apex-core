/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Example module with a few ports and configuration properties.
 */
public class MyProtoModule<T extends Object> extends ProtoModule {

  /**
   * Example for (runtime) typed input port.
   * The type information is retained at runtime and can be used for validation by the framework.
   */
  @ProtoInputPortFieldAnnotation(name="port1")
  public transient InputPort<String> inport1 = new InputPort<String>(this) {
    @Override
    final public void process(String payload) {
    }
  };

  /**
   * Untyped input port implemented using anonymous class
   * The port is untyped because it is using the enclosing classes type parameter.
   */
  @ProtoInputPortFieldAnnotation(name="port2")
  public transient InputPort<T> inport2 = new InputPort<T>(this) {
    @Override
    final public void process(T payload) {
      /*
      try {
        Class<?> c = this.getClass();
        Method method = c.getDeclaredMethod("process", Object.class);
        System.out.println("payload class: " + payload.getClass() + " method: " + method);
      } catch (Exception e) {
      }
      */
      if (outport1.isConnected()) {
          HashMap<String, T> m = new HashMap<String, T>();
          m.put("payload", payload);
          outport1.emit(m);
      }
    }
  };

  @ProtoOutputPortFieldAnnotation(name="outport1")
  final transient OutputPort<Map<String, T>> outport1 = new OutputPort<Map<String,T>>(this);

  @ProtoOutputPortFieldAnnotation(name="outport2")
  final transient OutputPort<byte[]> outport2 = new OutputPort<byte[]>(this);

  // just to try it out
  @ProtoInputPortProcessAnnotation(name="methodAnnotatedPort1")
  public void processPort1(String s) {

  }

  @Override
  public void processGeneric(Object payload) {
  }

  private String myConfigField;

  public String getMyConfigField() {
    return myConfigField;
  }

  public void setMyConfigField(String myConfigField) {
    this.myConfigField = myConfigField;
  }

}

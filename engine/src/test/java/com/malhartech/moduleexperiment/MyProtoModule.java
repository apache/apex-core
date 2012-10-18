/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import java.util.HashMap;
import java.util.Map;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 * Example module with a few ports and configuration properties.
 */
public class MyProtoModule<T extends Object> extends BaseOperator {

  /**
   * Example for (runtime) typed input port.
   * The type information is retained at runtime and can be used for validation by the framework.
   */
  @InputPortFieldAnnotation(name="port1")
  final public transient InputPort<String> inport1 = new DefaultInputPort<String>(this) {
    @Override
    final public void process(String payload) {
    }
  };

  /**
   * Untyped input port implemented using anonymous class
   * The port is untyped because it is using the enclosing classes type parameter.
   */
  @InputPortFieldAnnotation(name="port2")
  final public transient InputPort<T> inport2 = new DefaultInputPort<T>(this) {
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

  @OutputPortFieldAnnotation(name="outport1")
  final transient DefaultOutputPort<Map<String, T>> outport1 = new DefaultOutputPort<Map<String,T>>(this);

  @OutputPortFieldAnnotation(name="outport2")
  final transient DefaultOutputPort<byte[]> outport2 = new DefaultOutputPort<byte[]>(this);

  // just to try it out
  @ProtoInputPortProcessAnnotation(name="methodAnnotatedPort1")
  public void processPort1(String s) {

  }

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

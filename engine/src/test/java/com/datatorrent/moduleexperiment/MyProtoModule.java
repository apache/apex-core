/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.moduleexperiment;

import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import java.util.HashMap;
import java.util.Map;

/**
 * Example module with a few ports and configuration properties.
 */
public class MyProtoModule<T extends Object> extends BaseOperator {

  /**
   * Example for (runtime) typed input port.
   * The type information is retained at runtime and can be used for validation by the framework.
   */
  @InputPortFieldAnnotation(name="port1", optional=true)
  final public transient InputPort<String> inport1 = new DefaultInputPort<String>() {
    @Override
    final public void process(String payload) {
    }
  };

  /**
   * Untyped input port implemented using anonymous class
   * The port is untyped because it is using the enclosing classes type parameter.
   */
  @InputPortFieldAnnotation(name="port2", optional=true)
  final public transient InputPort<T> inport2 = new DefaultInputPort<T>() {
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

  @OutputPortFieldAnnotation(name="outport1", optional=true)
  final transient DefaultOutputPort<Map<String, T>> outport1 = new DefaultOutputPort<Map<String,T>>();

  @OutputPortFieldAnnotation(name="outport2", optional=true)
  final transient DefaultOutputPort<byte[]> outport2 = new DefaultOutputPort<byte[]>();

  @OutputPortFieldAnnotation(name="outport3", optional=true)
  final transient DefaultOutputPort<String> outport3 = new DefaultOutputPort<String>();

  private String myConfigField;

  public String getMyConfigField() {
    return myConfigField;
  }

  public void setMyConfigField(String myConfigField) {
    this.myConfigField = myConfigField;
  }

}

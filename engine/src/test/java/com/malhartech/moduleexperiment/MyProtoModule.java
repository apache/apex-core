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

  private static class MyInputPort implements InputPort<String> {
    @Override
    public void process(String payload) {
    }
  }

  @ProtoInputPortGetAnnotation(name="port1")
  public InputPort<String> getPort1() {
    // anonymous class vs static class makes no difference
    /*
    return new InputPort<String>() {
      @Override
      final public void process(String payload) {
      }
    };
    */
    return new MyInputPort();
  }

  @ProtoInputPortGetAnnotation(name="port2")
  public InputPort<T> getPort2() {
    return new InputPort<T>() {
      @Override
      final public void process(T payload) {
        HashMap<String, T> m = new HashMap<String, T>();
        m.put("payload", payload);
        MyProtoModule.this.outport1.emit(m);
      }
    };
  }

}

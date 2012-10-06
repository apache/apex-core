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
public class MyProtoModule implements ProtoModule {

  @ProtoOutputPortFieldAnnotation(name="outport1")
  private OutputPort<Map<String, String>> outport1;

  @ProtoOutputPortFieldAnnotation(name="outport2")
  private OutputPort<byte[]> outport2;

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
  public InputPort<String> getPort2() {
    return new InputPort<String>() {
      @Override
      final public void process(String payload) {
        HashMap<String, String> m = new HashMap<String, String>();
        m.put(payload, payload);
        MyProtoModule.this.outport1.emit(m);
      }
    };
  }

}

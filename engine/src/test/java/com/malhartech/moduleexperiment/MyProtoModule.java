/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

/**
 *
 */
public class MyProtoModule implements ProtoModule {

  @Override
  public void beginWindow() {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow() {
    // TODO Auto-generated method stub

  }

  @Override
  @ProtoInputPortProcessAnnotation(name="port1")
  public void processPort1(String s) {

  }

  @ProtoInputPortProcessAnnotation(name="port2")
  public void processPort2() {

  }

  private static class MyInputPort implements InputPort<String> {
    @Override
    public void process(String payload) {
    }
  }

  @ProtoInputPortGetAnnotation(name="port3")
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

}

/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.junit.Ignore;
import org.junit.Test;

import scala.actors.threadpool.Arrays;

import com.malhartech.dag.Sink;
import com.malhartech.moduleexperiment.ProtoModule.InputPort;
import com.malhartech.moduleexperiment.ProtoModule.OutputPort;

/**
 *
 */
public class ProtoModuleTest {

  int callCount = 100 * 1000 * 1000;

  @Test
  public void testDirectProcessCall() throws Exception {
    ProtoModule module = MyProtoModule.class.newInstance();
    long startTimeMillis = System.currentTimeMillis();
    for (int i=0; i<callCount; i++) {
      module.processGeneric("hello");
    }
    System.out.println(callCount + " direct process calls took " + (System.currentTimeMillis() - startTimeMillis) + " ms");
  }

  private static Method getInputPortMethod(String portName, Class<?> moduleClazz) {

    Method[] methods = moduleClazz.getDeclaredMethods();

    for (Method m : methods) {
      ProtoInputPortProcessAnnotation pa = m.getAnnotation(ProtoInputPortProcessAnnotation.class);
      if (pa != null && portName.equals(pa.name())) {
        // check parameter count and type
        Class<?>[] paramTypes = m.getParameterTypes();
        if (paramTypes.length != 1) {
          throw new IllegalArgumentException("Port processor method " + m + " should declare single parameter but found " + Arrays.asList(paramTypes));
        }
        // TODO: type check
        return m;
      }
    }
    throw new IllegalArgumentException("No port processor method found in class " + moduleClazz + " for " + portName);
  }

  /**
   * Method calls through reflection are much slower (600x+)
   * @throws Exception
   */
  @Ignore
  @Test
  public void testInputPortMethodAnnotation() throws Exception {

    String portName = "methodAnnotatedPort1";
    MyProtoModule module = new MyProtoModule();
    Method m = getInputPortMethod(portName, module.getClass());
    long startTimeMillis = System.currentTimeMillis();
    for (int i=0; i<callCount; i++) {
      m.invoke(module, "hello");
    }
    System.out.println(callCount + " dynamic method calls took " + (System.currentTimeMillis() - startTimeMillis) + " ms");
  }

  private static <T> ProtoModule.InputPort<T> getInputPortInterface(ProtoModule module, String portName, Class<T> portTypeClazz) throws Exception {

    Class<?> moduleClass = module.getClass();
    Method[] methods = moduleClass.getDeclaredMethods();
    for (Method m : methods) {
      ProtoInputPortGetAnnotation ga = m.getAnnotation(ProtoInputPortGetAnnotation.class);
      if (ga != null && portName.equals(ga.name())) {
        // check parameter count and type
        Class<?>[] paramTypes = m.getParameterTypes();
        if (paramTypes.length != 0) {
          throw new IllegalArgumentException("Port factory method " + m + " should declare no parameters but found " + Arrays.asList(paramTypes));
        }

        Class<?> returnType = m.getReturnType();
        if (!ProtoModule.InputPort.class.isAssignableFrom(returnType)) {
          throw new IllegalArgumentException("Port factory method " + m + " needs to return instance of " + ProtoModule.InputPort.class + " but found " + returnType);
        }

        // TODO: port type check
        return (ProtoModule.InputPort<T>)m.invoke(module);
      }
    }
    throw new IllegalArgumentException("Port processor factory method not found in " + module + " for " + portName);
  }

  private static void injectSink(ProtoModule module, String portName, Sink sink) throws Exception {
    Field[] fields = module.getClass().getDeclaredFields();
    for (int i = 0; i < fields.length; i++) {
      Field field = fields[i];
      ProtoOutputPortFieldAnnotation a = field.getAnnotation(ProtoOutputPortFieldAnnotation.class);
      if (a != null && portName.equals(a.name())) {
        field.setAccessible(true);
        Object outPort = field.get(module);
        if (outPort == null) {
          throw new IllegalArgumentException("port is null " + field);
        }
        if (!(outPort instanceof OutputPort)) {
          throw new IllegalArgumentException("port is not of type " + OutputPort.class.getName());
        }
        ((OutputPort<?>)outPort).setSink(sink);
        return;
      }
    }
    throw new IllegalArgumentException("Failed to inject sink for port " + portName);
  }

  /**
   * Calls port interface created by module.
   * Would have expected this to be equivalent to direct call, but it takes 2x
   * @throws Exception
   */
  @Test
  public void testInputPortInterfaceAnnotation() throws Exception {

    String portName = "port1";
    MyProtoModule module = new MyProtoModule();
    InputPort<String> portObject = getInputPortInterface(module, portName, String.class);

    long startTimeMillis = System.currentTimeMillis();
    for (int i=0; i<callCount; i++) {
      portObject.process("hello");
    }
    System.out.println(callCount + " port interface calls took " + (System.currentTimeMillis() - startTimeMillis) + " ms");
  }


  @Test
  public void testOutputPortAnnotation() throws Exception {

    MyProtoModule<String> module = new MyProtoModule<String>();
    InputPort<String> inport = getInputPortInterface(module, "port2", String.class);

    // inject (untyped) sink
    Sink sink = new Sink() {
      @Override
      public void process(Object payload) {
        System.out.println("sink: " + payload);
      }
    };

    injectSink(module, "outport1", sink);

    inport.process("hello");

  }


}


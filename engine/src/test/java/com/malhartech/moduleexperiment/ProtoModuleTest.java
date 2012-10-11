/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import junit.framework.Assert;

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

  public static interface GenericInterface<T> {
  }

  public static interface StringTypedInterface extends GenericInterface<String> {
  }

  public static class GenericInterfaceImpl<T> implements GenericInterface<T> {
  }

  /**
   * Typed input port. The type information is retained at runtime and can be used for validation by the framework.
   */
  public static class StringType1 implements GenericInterface<String> {
  }

  public static class StringType2 implements StringTypedInterface {
  }

  public static class StringType3 extends GenericInterfaceImpl<String> {
  }


  @Test
  public void testTypeDiscovery() {

    Assert.assertEquals("", String.class, findTypeArgument(StringType1.class, GenericInterface.class));
    Assert.assertEquals("", String.class, findTypeArgument(StringType2.class, GenericInterface.class));
    Assert.assertEquals("", String.class, findTypeArgument(StringType3.class, GenericInterface.class));
    Assert.assertEquals("", "T", ""+findTypeArgument(GenericInterfaceImpl.class, GenericInterface.class));

  }

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

  static Type getTypeArgument(Type type, Class<?> rawType) {
    if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType)type;
      if (rawType.isAssignableFrom((Class<?>)ptype.getRawType())) {
        //System.out.println("   ptype: rawtype=" + ptype.getRawType() + "  " + Arrays.asList(ptype.getActualTypeArguments()));
        return ptype.getActualTypeArguments()[0];
      }
    }
    return null;
  }


  /**
   * Find the type argument for a given class and parameterized interface
   * the is implemented directly or in a super class or super interface.
   * @param c
   * @param genericInterfaceClass
   * @return
   */
  public static Type findTypeArgument(Class<?> c, Class<?> genericInterfaceClass) {
    while (c != null) {
      // extends generic class?
      Type t = getTypeArgument(c.getGenericSuperclass(), genericInterfaceClass);
      if (t != null) {
        return t;
      }
      // implemented interfaces
      Type[] types = c.getGenericInterfaces();
      for (Type interfaceType : types) {
        if ((t = getTypeArgument(interfaceType, genericInterfaceClass)) != null) {
          return t;
        }
      }
      // interface that extends parameterized interface?
      for (Class<?> ifClass : c.getInterfaces()) {
        types = ifClass.getGenericInterfaces();
        for (Type interfaceType : types) {
          if ((t = getTypeArgument(interfaceType, genericInterfaceClass)) != null) {
            return t;
          }
        }
      }
      c = c.getSuperclass();
    }
    return null;
  }

  private static <T> ProtoModule.InputPort<T> getInputPortInterface(ProtoModule module, String portName, Class<T> portTypeClazz) throws Exception {

    Field[] fields = module.getClass().getDeclaredFields();
    for (Field field : fields) {
      ProtoInputPortGetAnnotation a = field.getAnnotation(ProtoInputPortGetAnnotation.class);
      if (a != null && portName.equals(a.name())) {
        field.setAccessible(true);

        Object portObject = field.get(module);
        if (!(portObject instanceof InputPort)) {
          throw new IllegalArgumentException("Port field " + field + " needs to be of type " + ProtoModule.InputPort.class + " but found " + portObject.getClass().getName());
        }

        Type genericType = findTypeArgument(portObject.getClass(), InputPort.class);
        System.out.println(portName + " type is: " + genericType);

        return (ProtoModule.InputPort<T>)portObject;
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
  public void testInputPortAnnotation() throws Exception {

    MyProtoModule<?> module = new MyProtoModule<Integer>();

    InputPort<String> port1 = getInputPortInterface(module, "port1", String.class);
    InputPort<String> port2 = getInputPortInterface(module, "port2", String.class);

    long startTimeMillis = System.currentTimeMillis();
    for (int i=0; i<callCount; i++) {
      //port1.process("hello");
      port2.process("hello2");
    }
    System.out.println(callCount + " port interface calls took " + (System.currentTimeMillis() - startTimeMillis) + " ms");
  }


  @Test
  public void testOutputPortAnnotation() throws Exception {

    MyProtoModule<?> module = new MyProtoModule<String>();
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
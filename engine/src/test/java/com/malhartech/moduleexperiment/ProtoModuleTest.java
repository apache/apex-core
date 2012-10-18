/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.TypeLiteral;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Sink;
import com.malhartech.dag.DefaultModuleSerDe;
//import com.malhartech.dag.TestSink;

/**
 *
 */
public class ProtoModuleTest {
  private static Logger LOG = LoggerFactory.getLogger(ProtoModuleTest.class);


  int callCount = 100 * 1000 * 1000;

  public static Type getParameterizedTypeArgument(Type type, Class<?> rawType) {
    if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType)type;
      if (rawType.isAssignableFrom((Class<?>)ptype.getRawType())) {
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
      Type t = getParameterizedTypeArgument(c.getGenericSuperclass(), genericInterfaceClass);
      if (t != null) {
        return t;
      }
      // implemented interfaces
      Type[] types = c.getGenericInterfaces();
      for (Type interfaceType : types) {
        if ((t = getParameterizedTypeArgument(interfaceType, genericInterfaceClass)) != null) {
          return t;
        }
      }
      // interface that extends parameterized interface?
      for (Class<?> ifClass : c.getInterfaces()) {
        types = ifClass.getGenericInterfaces();
        for (Type interfaceType : types) {
          if ((t = getParameterizedTypeArgument(interfaceType, genericInterfaceClass)) != null) {
            return t;
          }
        }
      }
      c = c.getSuperclass();
    }
    return null;
  }

  private static interface GenericInterface<T> {
  }

  private static interface StringTypedInterface extends GenericInterface<String> {
  }

  private static class GenericClass<T extends Map<String, String>> implements GenericInterface<T> {
  }

  /**
   * Typed input port. The type information is retained at runtime and can be used for validation by the framework.
   */
  private static class StringType1 implements GenericInterface<String> {
  }

  private static class StringType2 implements StringTypedInterface {
  }

  private static class MapStringStringType extends GenericClass<Map<String, String>> {
  }


  @Test
  public void testTypeDiscovery() {

    Assert.assertEquals("", String.class, findTypeArgument(StringType1.class, GenericInterface.class));
    Assert.assertEquals("", String.class, findTypeArgument(StringType2.class, GenericInterface.class));

    Type t = findTypeArgument(MapStringStringType.class, GenericInterface.class);
    Assert.assertTrue("instanceof ParameterizedType " + t, t instanceof ParameterizedType);
    ParameterizedType ptype = (ParameterizedType)t;
    Assert.assertEquals("", Map.class, ptype.getRawType());
    Assert.assertEquals("", 2, ptype.getActualTypeArguments().length);
    Assert.assertEquals("", String.class, ptype.getActualTypeArguments()[0]);
    Assert.assertEquals("", String.class, ptype.getActualTypeArguments()[1]);

    Assert.assertEquals("", "T", ""+findTypeArgument(GenericClass.class, GenericInterface.class));

  }

  @Test
  public void testDirectProcessCall() throws Exception {
    MyProtoModule<?> module = MyProtoModule.class.newInstance();
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
    MyProtoModule<?> module = new MyProtoModule<Object>();
    Method m = getInputPortMethod(portName, module.getClass());
    long startTimeMillis = System.currentTimeMillis();
    for (int i=0; i<callCount; i++) {
      m.invoke(module, "hello");
    }
    LOG.debug(callCount + " dynamic method calls took " + (System.currentTimeMillis() - startTimeMillis) + " ms");
  }

  private static <T> NewOperator.InputPort<T> getInputPortInterface(NewOperator module, String portName, Class<T> portTypeClazz) throws Exception {

    Field[] fields = module.getClass().getDeclaredFields();
    for (Field field : fields) {
      InputPortFieldAnnotation a = field.getAnnotation(InputPortFieldAnnotation.class);
      if (a != null && portName.equals(a.name())) {
        field.setAccessible(true);

        Object portObject = field.get(module);
        if (!(portObject instanceof InputPort)) {
          throw new IllegalArgumentException("Port field " + field + " needs to be of type " + NewOperator.InputPort.class + " but found " + portObject.getClass().getName());
        }

        Type genericType = findTypeArgument(portObject.getClass(), InputPort.class);
        LOG.debug(portName + " type is: " + genericType);

        return (NewOperator.InputPort<T>)portObject;
      }
    }
    throw new IllegalArgumentException("Port processor factory method not found in " + module + " for " + portName);
  }

  private static void injectSink(NewOperator module, String portName, Sink<Object> sink) throws Exception {
    Field[] fields = module.getClass().getDeclaredFields();
    for (int i = 0; i < fields.length; i++) {
      Field field = fields[i];
      OutputPortFieldAnnotation a = field.getAnnotation(OutputPortFieldAnnotation.class);
      if (a != null && portName.equals(a.name())) {
        field.setAccessible(true);
        Object outPort = field.get(module);
        if (outPort == null) {
          throw new IllegalArgumentException("port is null " + field);
        }
        if (!(outPort instanceof DefaultOutputPort)) {
          throw new IllegalArgumentException("port is not of type " + DefaultOutputPort.class.getName());
        }
        ((DefaultOutputPort<Object>)outPort).setSink(sink);
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
    Sink<String> s = port2.getSink();
    for (int i=0; i<callCount; i++) {
      //port1.process("hello");
      s.process("hello2");
    }
    LOG.debug(callCount + " port interface calls took " + (System.currentTimeMillis() - startTimeMillis) + " ms");
  }


  @Test
  public void testOutputPortAnnotation() throws Exception {

    MyProtoModule<?> module = new MyProtoModule<String>();
    InputPort<String> inport = getInputPortInterface(module, "port2", String.class);

    // inject (untyped) sink
    Sink<Object> sink = new Sink<Object>() {
      @Override
      public void process(Object payload) {
        LOG.debug("sink: " + payload);
      }
    };

    injectSink(module, "outport1", sink);

    inport.getSink().process("hello");

  }

  @Test
  public void testSerialization() {
    DefaultModuleSerDe serde = new DefaultModuleSerDe();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    serde.write(new MyProtoModule<Object>(), bos);
    serde.read(new ByteArrayInputStream(bos.toByteArray()));
  }


  @Test
  public void testDAG() throws Exception {

    DAG dag = new DAG();

    MyProtoModule<Object> m1 = new MyProtoModule<Object>();
    m1.setName("operator1");
    m1.setMyConfigField("someField");

    m1.inport2.getSink().process("something");

    MyProtoModule<Object> m2 = new MyProtoModule<Object>();
    m2.setName("operator2");

    MyProtoModule<Object> m3 = new MyProtoModule<Object>();
    m3.setName("operator3");

    // module instances are added to teh DAG as ports are referenced
    // and establish module-ports relationship
    dag.addOperator(m1);
    //Operator operator2 = dag.addOperator("operator2", m2);
    //Operator operator3 = dag.addOperator("operator3", m3);

    dag.addStream("stream1", m1.outport1, m2.inport1);
    dag.addStream("stream2", m2.outport1, m3.inport1);
    Assert.assertEquals("" + dag.getAllOperators(), 3, dag.getAllOperators().size());

    dag.validate();

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DAG.write(dag, outStream);
    outStream.close();

    byte[] dagBytes = outStream.toByteArray();
    LOG.debug("dag bytes size: " + dagBytes.length);
    DAG clonedDag = DAG.read(new ByteArrayInputStream(dagBytes));
    Assert.assertEquals(dag.getAllOperators().size(), clonedDag.getAllOperators().size());
    Operator clonedModule = clonedDag.getOperatorWrapper("operator1").getModule();
    Assert.assertNotNull("", clonedModule);
    Assert.assertEquals(""+m1.getMyConfigField(), m1.getMyConfigField(), ((MyProtoModule<?>)clonedModule).getMyConfigField());
    clonedDag.validate();
  }

  @Test
  public void testProtoArithmeticQuotient() throws Exception {

    ProtoArithmeticQuotient node = new ProtoArithmeticQuotient();
    node.setMultiplyBy(2);

//    TestSink<HashMap<String, Number>> testSink = new TestSink<HashMap<String, Number>>();
//    node.outportQuotient.setSink(testSink);

    LOG.debug("type inportNumerator: " + findTypeArgument(node.inportNumerator.getClass(), InputPort.class));
    LOG.debug("type inportDenominator: " + findTypeArgument(node.inportDenominator.getClass(), InputPort.class));
    LOG.debug("type outportQuotient: " + findTypeArgument(node.outportQuotient.getClass(), DefaultOutputPort.class));

    HashMap<String, Number> ninput = null;
    HashMap<String, Number> dinput = null;

    int numtuples = 1000;
    for (int i = 0; i < numtuples; i++) {
      ninput = new HashMap<String, Number>();
      dinput = new HashMap<String, Number>();
      ninput.put("a", 2);
      ninput.put("b", 20);
      ninput.put("c", 1000);
//      node.inportNumerator.process(ninput);
      dinput.put("a", 2);
      dinput.put("b", 40);
      dinput.put("c", 500);
//      node.inportDenominator.process(dinput);
    }
    node.endWindow();
//    LOG.debug("output tuples: " + testSink.collectedTuples);

  }

  @Test
  public void testTypeLiteral() throws Exception {
    TypeLiteral<Map<Integer, String>> mapType
        = new TypeLiteral<Map<Integer, String>>() {};
    TypeLiteral<?> keySetType
        = mapType.getReturnType(Map.class.getMethod("keySet"));
    System.out.println(keySetType);
  }

}

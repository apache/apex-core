/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import com.google.inject.TypeLiteral;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ProtoModuleTest {
  private static Logger LOG = LoggerFactory.getLogger(ProtoModuleTest.class);


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

  private static class StringOutputPort extends DefaultOutputPort<String> {
    public StringOutputPort(Operator operator) {
      super(operator);
    }
  }

  static class ParameterizedOperator<T0, T1 extends Map<String, ? extends T0>, T2 extends Number> extends BaseOperator implements GenericInterface<T1> {
    final InputPort<T1> inputT1 = new DefaultInputPort<T1>(this) {
      @Override
      public void process(T1 tuple) {
      }
    };
    final OutputPort<T2> outportT2 = new DefaultOutputPort<T2>(this);
    final OutputPort<Number> outportNumberParam = new DefaultOutputPort<Number>(this);
    final StringOutputPort outportString = new StringOutputPort(this);
  }

  public static Type getPortType(Field f) {
    if (f.getGenericType() instanceof ParameterizedType) {
      ParameterizedType t = (ParameterizedType)f.getGenericType();
      //LOG.debug("Field type is parameterized: " + Arrays.asList(t.getActualTypeArguments()));
      //LOG.debug("rawType: " + t.getRawType()); // the port class
      Type typeArgument = t.getActualTypeArguments()[0];
      if (typeArgument instanceof Class) {
         return typeArgument;
      } else if (typeArgument instanceof TypeVariable) {
        TypeVariable<?> tv = (TypeVariable<?>)typeArgument;
        LOG.debug("bounds: " + Arrays.asList(tv.getBounds()));
        // variable may contain other variables, java.util.Map<java.lang.String, ? extends T2>
        return tv.getBounds()[0];
      } else {
        // ports are always parameterized
        throw new IllegalArgumentException("No type variable: " + typeArgument + ", typeParameters: " + Arrays.asList(f.getClass().getTypeParameters()));
      }
    } else {
      LOG.debug("Field is not parameterized: " + f.getGenericType());
      if (Operator.Port.class.isAssignableFrom(f.getType())) {
        Type t = findTypeArgument(f.getType(), Operator.Port.class);
        return t;
      }
      throw new IllegalArgumentException("Cannot determine type argument for field " + f);
    }
  }

  @Test
  public void testTypeDiscovery() throws Exception {
/*
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
*/
    for (Field f : ParameterizedOperator.class.getDeclaredFields()) {
      Type t = getPortType(f);
      System.out.println("** Field: " + f.getName() + " has type: " + t);
    }

    Type typeInputT1 = getPortType(ParameterizedOperator.class.getDeclaredField("inputT1"));
    Assert.assertNotNull(typeInputT1);
    Assert.assertTrue("instanceof ParameterizedType " + typeInputT1, typeInputT1 instanceof ParameterizedType);
    ParameterizedType ptype = (ParameterizedType)typeInputT1;
    Assert.assertEquals("ownertype " + typeInputT1, Map.class, ptype.getRawType());
    Type[] typeArgs = ptype.getActualTypeArguments();
    Assert.assertEquals("typeArgs[0] " + ptype, String.class, typeArgs[0]);
    WildcardType wt = ((WildcardType)typeArgs[1]);
    Assert.assertEquals("typeArgs[1] " + ptype, "T0", wt.getUpperBounds()[0].toString());
    TypeVariable<?> tv = (TypeVariable<?>)wt.getUpperBounds()[0];
    Assert.assertEquals("bounds[0] " + tv, Object.class, tv.getBounds()[0]);
  }

  @Test
  public void testDAG() throws Exception {

    DAG dag = new DAG();

    MyProtoModule<Object> m1 = dag.addOperator("operator1", new MyProtoModule<Object>());
    m1.setMyConfigField("someField");

    m1.inport2.getSink().process("something");

    MyProtoModule<Object> m2 = dag.addOperator("operator2", new MyProtoModule<Object>());

    MyProtoModule<Object> m3 = dag.addOperator("operator3", new MyProtoModule<Object>());

    dag.addStream("stream1", m1.outport3, m2.inport1);
    dag.addStream("stream2", m2.outport3, m3.inport1);
    Assert.assertEquals("" + dag.getAllOperators(), 3, dag.getAllOperators().size());

    dag.validate();

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DAG.write(dag, outStream);
    outStream.close();

    byte[] dagBytes = outStream.toByteArray();
    LOG.debug("dag bytes size: " + dagBytes.length);
    DAG clonedDag = DAG.read(new ByteArrayInputStream(dagBytes));
    Assert.assertEquals(dag.getAllOperators().size(), clonedDag.getAllOperators().size());
    Operator clonedModule = clonedDag.getOperatorMeta("operator1").getOperator();
    Assert.assertNotNull("", clonedModule);
    Assert.assertEquals(""+m1.getMyConfigField(), m1.getMyConfigField(), ((MyProtoModule<?>)clonedModule).getMyConfigField());
    clonedDag.validate();
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

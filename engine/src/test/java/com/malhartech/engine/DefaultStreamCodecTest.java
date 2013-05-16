/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.malhartech.api.DefaultOperatorSerDe;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.engine.DefaultStreamCodec.ClassIdPair;
import com.malhartech.util.KryoJdkSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import com.malhartech.common.Fragment;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class DefaultStreamCodecTest
{
  static class TestClass
  {
    final String s;
    final int i;

    TestClass(String s, int i)
    {
      this.s = s;
      this.i = i;
    }

    TestClass()
    {
      s = "default!";
      i = Integer.MAX_VALUE;
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 97 * hash + (this.s != null ? this.s.hashCode() : 0);
      hash = 97 * hash + this.i;
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final TestClass other = (TestClass)obj;
      if ((this.s == null) ? (other.s != null) : !this.s.equals(other.s)) {
        return false;
      }
      if (this.i != other.i) {
        return false;
      }
      return true;
    }

  }

  public DefaultStreamCodecTest()
  {
  }

  @Test
  public void testVirginKryo()
  {
    Kryo coder = new Kryo();
    Kryo decoder = new Kryo();

    ClassIdPair cip = new ClassIdPair();

    Output output = new Output(4096, Integer.MAX_VALUE);
    coder.writeClassAndObject(output, cip);

    Input input = new Input();
    input.setBuffer(output.toBytes());
    decoder.readClassAndObject(input);
  }

  @Test
  public void testString()
  {
    StreamCodec<Object> coder = new DefaultStreamCodec<Object>();
    StreamCodec<Object> decoder = new DefaultStreamCodec<Object>();

    String hello = "hello";

    DataStatePair dsp = coder.toByteArray(hello);
    Assert.assertEquals("both are hello", hello, decoder.fromByteArray(dsp));
  }

  @Test
  public void testCustomObject()
  {
    DefaultStreamCodec<Object> coder = new DefaultStreamCodec<Object>();
    DefaultStreamCodec<Object> decoder = new DefaultStreamCodec<Object>();

    TestClass tc = new TestClass("hello!", 42);
    //String tc = "hello";

    DataStatePair dsp1 = coder.toByteArray(tc);
    Fragment state1 = dsp1.state;
    DataStatePair dsp2 = coder.toByteArray(tc);
    Fragment state2 = dsp2.state;
    assert (state1 != null);
    assert (state2 == null);
    Assert.assertEquals(dsp1.data, dsp2.data);

    Object tcObject1 = decoder.fromByteArray(dsp1);
    assert (tc.equals(tcObject1));

    Object tcObject2 = decoder.fromByteArray(dsp2);
    assert (tc.equals(tcObject2));

    coder.resetState();

    dsp2 = coder.toByteArray(tc);
    state2 = dsp2.state;
    Assert.assertEquals(state1, state2);

    dsp1 = coder.toByteArray(tc);
    dsp2 = coder.toByteArray(tc);
    Assert.assertEquals(dsp1.data, dsp2.data);
    Assert.assertEquals(dsp1.state, dsp2.state);
  }

  public static class TestTuple
  {
    final Integer finalField;

    @SuppressWarnings("unused")
    private TestTuple()
    {
      finalField = null;
    }

    public TestTuple(Integer i)
    {
      this.finalField = i;
    }

  }

  @Test
  public void testFinalFieldSerialization() throws Exception
  {
    TestTuple t1 = new TestTuple(5);
    DefaultStreamCodec<Object> c = new DefaultStreamCodec<Object>();
    DataStatePair dsp = c.toByteArray(t1);
    TestTuple t2 = (TestTuple)c.fromByteArray(dsp);
    Assert.assertEquals("", t1.finalField, t2.finalField);
  }

  @DefaultSerializer(KryoJdkSerializer.class)
  public static class OuterClass implements Serializable
  {
    private static final long serialVersionUID = -3128672061060284420L;

    @DefaultSerializer(KryoJdkSerializer.class)
    public class InnerClass implements Serializable
    {
      private static final long serialVersionUID = -7176523451391231326L;
    }

  }

  @Test
  public void testInnerClassSerialization() throws Exception
  {
    OuterClass outer = new OuterClass();
    Object inner = outer.new InnerClass();

    for (Object o: new Object[] {outer, inner}) {
      DefaultStreamCodec<Object> c = new DefaultStreamCodec<Object>();
      DataStatePair dsp = c.toByteArray(o);
      c.fromByteArray(dsp);

      DefaultOperatorSerDe os = new DefaultOperatorSerDe();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      os.write(o, bos);
      os.read(new ByteArrayInputStream(bos.toByteArray()));
    }
  }

}

/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.Assert;
import org.junit.Test;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.codec.KryoJdkSerializer;

import com.datatorrent.common.util.Slice;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec.ClassIdPair;
import com.datatorrent.stram.codec.StatefulStreamCodec.DataStatePair;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class DefaultStatefulStreamCodecTest
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

  public DefaultStatefulStreamCodecTest()
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
    StatefulStreamCodec<Object> coder = new DefaultStatefulStreamCodec<Object>();
    StatefulStreamCodec<Object> decoder = new DefaultStatefulStreamCodec<Object>();

    String hello = "hello";

    DataStatePair dsp = coder.toDataStatePair(hello);
    Assert.assertEquals("both are hello", hello, decoder.fromDataStatePair(dsp));
  }

  @Test
  public void testCustomObject()
  {
    DefaultStatefulStreamCodec<Object> coder = new DefaultStatefulStreamCodec<Object>();
    DefaultStatefulStreamCodec<Object> decoder = new DefaultStatefulStreamCodec<Object>();

    TestClass tc = new TestClass("hello!", 42);
    //String tc = "hello";

    DataStatePair dsp1 = coder.toDataStatePair(tc);
    Slice state1 = dsp1.state;
    DataStatePair dsp2 = coder.toDataStatePair(tc);
    Slice state2 = dsp2.state;
    assert (state1 != null);
    assert (state2 == null);
    Assert.assertEquals(dsp1.data, dsp2.data);

    Object tcObject1 = decoder.fromDataStatePair(dsp1);
    assert (tc.equals(tcObject1));

    Object tcObject2 = decoder.fromDataStatePair(dsp2);
    assert (tc.equals(tcObject2));

    coder.resetState();

    dsp2 = coder.toDataStatePair(tc);
    state2 = dsp2.state;
    Assert.assertEquals(state1, state2);

    dsp1 = coder.toDataStatePair(tc);
    dsp2 = coder.toDataStatePair(tc);
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
    DefaultStatefulStreamCodec<Object> c = new DefaultStatefulStreamCodec<Object>();
    DataStatePair dsp = c.toDataStatePair(t1);
    TestTuple t2 = (TestTuple)c.fromDataStatePair(dsp);
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
      DefaultStatefulStreamCodec<Object> c = new DefaultStatefulStreamCodec<Object>();
      DataStatePair dsp = c.toDataStatePair(o);
      c.fromDataStatePair(dsp);

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(o);
      oos.close();

      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
      ois.readObject();
    }
  }

}

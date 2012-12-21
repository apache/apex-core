/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.engine.DefaultStreamCodec.ClassIdPair;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class DefaultStreamCodecTest
{
  private static final Logger logger = LoggerFactory.getLogger(DefaultStreamCodecTest.class);

  static class TestClass
  {
    final String s;
    final int i;

    public TestClass(String s, int i)
    {
      this.s = s;
      this.i = i;
    }

    public TestClass()
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
    ClassIdPair clone = (ClassIdPair)decoder.readClassAndObject(input);
  }

  @Test
  public void testSomeMethod()
  {
    DefaultStreamCodec coder = new DefaultStreamCodec();
    DefaultStreamCodec decoder = new DefaultStreamCodec();

    TestClass tc = new TestClass("hello!", 42);
//    String tc = "hello!";

    DataStatePair dsp1 = coder.toByteArray(tc);
    byte[] state1 = dsp1.state;
    DataStatePair dsp2 = coder.toByteArray(tc);
    byte[] state2 = dsp2.state;
    assert (state1 != null);
    assert (state2 == null);
    Assert.assertArrayEquals(dsp1.data, dsp2.data);

    Object tcObject1 = decoder.fromByteArray(dsp1);
    assert (tc.equals(tcObject1));

    Object tcObject2 = decoder.fromByteArray(dsp2);
    assert (tc.equals(tcObject2));

    coder.checkpoint();

    dsp2 = coder.toByteArray(tc);
    state2 = dsp2.state;
    Assert.assertArrayEquals(state1, state2);

    dsp1 = coder.toByteArray(tc);
    dsp2 = coder.toByteArray(tc);
    Assert.assertArrayEquals(dsp1.data, dsp2.data);
    Assert.assertArrayEquals(dsp1.state, dsp2.state);
  }

  public static class TestTuple {
    final Integer finalField;
    @SuppressWarnings("unused")
    private TestTuple() {
      finalField = null;
    }
    public TestTuple(Integer i) {
      this.finalField = i;
    }
  }

  @Test
  public void testFinalFieldSerialization() throws Exception {
    TestTuple t1 = new TestTuple(5);
    DefaultStreamCodec c = new DefaultStreamCodec();
    DataStatePair dsp = c.toByteArray(t1);
    TestTuple t2 = (TestTuple)c.fromByteArray(dsp);
    Assert.assertEquals("", t1.finalField, t2.finalField);
  }

}

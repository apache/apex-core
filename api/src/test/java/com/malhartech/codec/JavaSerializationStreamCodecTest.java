/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.codec;

import com.malhartech.common.Fragment;
import java.io.IOException;
import java.io.Serializable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class JavaSerializationStreamCodecTest
{
  static class TestClass implements Serializable
  {
    private static final long serialVersionUID = 201301081743L;
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

  @Test
  public void testSomeMethod() throws IOException
  {
    JavaSerializationStreamCodec<Serializable> coder = new JavaSerializationStreamCodec<Serializable>();
    JavaSerializationStreamCodec<Serializable> decoder = new JavaSerializationStreamCodec<Serializable>();

    TestClass tc = new TestClass("hello!", 42);

    Fragment dsp1 = coder.toByteArray(tc);
    Fragment dsp2 = coder.toByteArray(tc);
    Assert.assertEquals(dsp1, dsp2);

    Object tcObject1 = decoder.fromByteArray(dsp1);
    assert (tc.equals(tcObject1));

    Object tcObject2 = decoder.fromByteArray(dsp2);
    assert (tc.equals(tcObject2));

    dsp1 = coder.toByteArray(tc);
    dsp2 = coder.toByteArray(tc);
    Assert.assertEquals(dsp1, dsp2);
  }

  public static class TestTuple implements Serializable
  {
    private static final long serialVersionUID = 201301081744L;
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
    JavaSerializationStreamCodec<Serializable> c = new JavaSerializationStreamCodec<Serializable>();
    Fragment dsp = c.toByteArray(t1);
    TestTuple t2 = (TestTuple)c.fromByteArray(dsp);
    Assert.assertEquals("", t1.finalField, t2.finalField);
  }

  private static final Logger logger = LoggerFactory.getLogger(JavaSerializationStreamCodecTest.class);
}

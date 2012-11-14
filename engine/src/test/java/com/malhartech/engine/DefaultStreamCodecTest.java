/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

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
  public void testSomeMethod()
  {
    DefaultStreamCodec dsc = new DefaultStreamCodec();

//    TestClass tc = new TestClass("hello!", 42);
    String tc = "hello!";

    byte[] tcbytes1 = dsc.toByteArray(tc);
    Object tcObject1 = dsc.fromByteArray(tcbytes1);
    assert(tc.equals(tcObject1));

    byte[] tcbytes2 = dsc.toByteArray(tc);
    Object tcObject2 = dsc.fromByteArray(tcbytes2);
    assert(tc.equals(tcObject2));

    String s1 = new String(tcbytes1);
    String s2 = new String(tcbytes2);

//    assert(tcbytes1.length < tcbytes2.length);
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import com.datatorrent.bufferserver.packet.DataTuple;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec.ClassIdPair;
import com.datatorrent.stram.codec.StatefulStreamCodec.DataStatePair;

/**
 *
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
      return this.i == other.i;
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
    StatefulStreamCodec<Object> coder = new DefaultStatefulStreamCodec<>();
    StatefulStreamCodec<Object> decoder = new DefaultStatefulStreamCodec<>();

    String hello = "hello";

    DataStatePair dsp = coder.toDataStatePair(hello);
    Assert.assertEquals("both are hello", hello, decoder.fromDataStatePair(dsp));
  }

  @Test
  public void testCustomObject()
  {
    DefaultStatefulStreamCodec<TestClass> coder = new DefaultStatefulStreamCodec<>();
    DefaultStatefulStreamCodec<TestClass> decoder = coder.newInstance();

    TestClass tc = new TestClass("hello!", 42);
    //String tc = "hello";

    DataStatePair dsp = coder.toDataStatePair(tc);
    Assert.assertNotNull(dsp.state);
    byte[] state1 =  DataTuple.getSerializedTuple(MessageType.CODEC_STATE_VALUE, dsp.state);
    byte[] data1 = PayloadTuple.getSerializedTuple(0, dsp.data);

    dsp = coder.toDataStatePair(tc);
    Assert.assertNull(dsp.state);
    byte[] data2 = PayloadTuple.getSerializedTuple(0, dsp.data);

    Assert.assertNotSame(data1, data2);
    Assert.assertArrayEquals(data1, data2);

    dsp.state = Tuple.getTuple(state1, 0, state1.length).getData();
    dsp.data = Tuple.getTuple(data1, 0, data1.length).getData();
    Assert.assertEquals(tc, decoder.fromDataStatePair(dsp));

    dsp.state = null;
    dsp.data = Tuple.getTuple(data2, 0, data2.length).getData();
    Assert.assertEquals(tc, decoder.fromDataStatePair(dsp));

    coder.resetState();

    dsp = coder.toDataStatePair(tc);
    Assert.assertArrayEquals(state1, DataTuple.getSerializedTuple(MessageType.CODEC_STATE_VALUE, dsp.state));

    Assert.assertNull(coder.toDataStatePair(tc).state);
    data1 = PayloadTuple.getSerializedTuple(Integer.MAX_VALUE, coder.toDataStatePair(tc).data);
    data2 = PayloadTuple.getSerializedTuple(Integer.MAX_VALUE, coder.toDataStatePair(tc).data);
    Assert.assertArrayEquals(data1, data2);
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
    DefaultStatefulStreamCodec<Object> c = new DefaultStatefulStreamCodec<>();
    DataStatePair dsp = c.toDataStatePair(t1);
    TestTuple t2 = (TestTuple)c.fromDataStatePair(dsp);
    Assert.assertEquals("", t1.finalField, t2.finalField);
  }

  @DefaultSerializer(JavaSerializer.class)
  public static class OuterClass implements Serializable
  {
    private static final long serialVersionUID = -3128672061060284420L;

    @DefaultSerializer(JavaSerializer.class)
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
      DefaultStatefulStreamCodec<Object> c = new DefaultStatefulStreamCodec<>();
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

  static class NoNewInstanceStatefulStreamCodec<T> extends DefaultStatefulStreamCodec<T>
  {

  }

  @Test
  public void testNewInstanceMethod()
  {
    NoNewInstanceStatefulStreamCodec codec = new NoNewInstanceStatefulStreamCodec();
    DefaultStatefulStreamCodec newCodec = codec.newInstance();
    Assert.assertNotEquals("Codec and newCodec are not same ", codec, newCodec);
    Assert.assertEquals("Class of codec and newCodec is same ", newCodec.getClass(), codec.getClass());
  }
}

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
package com.datatorrent.common.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class SerializableObjectTest
{
  public static final String filename = "target/" + SerializableObjectTest.class.getName() + ".bin";

  public static class SerializableOperator<T> extends SerializableObject
  {
    public final transient InputPort<T> input = new DefaultInputPort<T>()
    {
      @Override
      public void process(T tuple)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

    };
    public final transient OutputPort<T> output = new DefaultOutputPort<>();
    private int i;

    public void setI(int i)
    {
      this.i = i;
    }

    public int getI()
    {
      return i;
    }

    @Override
    public int hashCode()
    {
      int hash = 3;
      hash = 97 * hash + (this.input != null ? this.input.hashCode() : 0);
      hash = 97 * hash + (this.output != null ? this.output.hashCode() : 0);
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

      @SuppressWarnings("unchecked")
      final SerializableOperator<T> other = (SerializableOperator<T>)obj;

      if (this.input == null || other.input == null) {
        return false;
      }

      if (this.output == null || other.output == null) {
        return false;
      }

      return this.i == other.i;
    }

    @Override
    public String toString()
    {
      return "SerializableOperator{" + "input=" + input + ", output=" + output + ", i=" + i + '}';
    }

    private static final long serialVersionUID = 201404140854L;
  }

  @Test
  public void testReadResolve() throws Exception
  {
    SerializableOperator<Object> pre = new SerializableOperator<>();
    pre.setI(10);

    FileOutputStream fos = new FileOutputStream(filename);
    ObjectOutputStream oos = new ObjectOutputStream(fos);
    oos.writeObject(pre);
    oos.close();

    FileInputStream fis = new FileInputStream(filename);
    ObjectInputStream ois = new ObjectInputStream(fis);
    Object post = ois.readObject();
    ois.close();

    assertEquals("Serialized Deserialized Objects", pre, post);
  }

}

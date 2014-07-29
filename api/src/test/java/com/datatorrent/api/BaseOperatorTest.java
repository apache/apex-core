/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.api;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class BaseOperatorTest
{
  public static final String filename = "target/" + BaseOperatorTest.class.getName() + ".bin";

  public static class SerializableOperator<T> extends BaseOperator implements Serializable
  {
    public final transient InputPort<T> input = new DefaultInputPort<T>()
    {
      @Override
      public void process(T tuple)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

    };
    public final transient OutputPort<T> output = new DefaultOutputPort<T>();
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

      if (this.i != other.i) {
        return false;
      }
      return true;
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
    SerializableOperator<Object> pre = new SerializableOperator<Object>();
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
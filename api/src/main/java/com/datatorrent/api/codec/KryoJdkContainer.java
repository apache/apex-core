/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api.codec;

import java.io.Serializable;

import com.esotericsoftware.kryo.DefaultSerializer;

/**
 * KryoJdkContainer wraps a Java serializable object and sets up a Kryo Serializer.
 *
 * This class implements a simple wrapper that is serializable by Kryo for a Java serializable object
 * that isn't directly serializable in Kryo. This could be for reasons such as the object not having a
 * default constructor or a member object in the the object not having a default constructor.<br>
 * <br>
 * This container can be used when the object code cannot be modified to use the
 * KryoJdkSerializer directly.<br>
 * <br>
 *
 * @param <T> - Type of the object which you would like to serialize using KryoJdkSerializer.
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
@DefaultSerializer(KryoJdkSerializer.class)
public class KryoJdkContainer<T> implements Serializable
{
  private static final long serialVersionUID = 201306031549L;
  private T t;

  public KryoJdkContainer()
  {
  }

  public KryoJdkContainer(T t)
  {
    this.t = t;
  }

  public void setComponent(T t)
  {
    this.t = t;
  }

  public T getComponent()
  {
    return t;
  }

  @Override
  public boolean equals(Object o)
  {
    boolean equal = false;
    if (o instanceof KryoJdkContainer) {
      KryoJdkContainer<?> k = (KryoJdkContainer<?>)o;
      equal = t.equals(k.getComponent());
    }
    return equal;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 29 * hash + (this.t != null ? this.t.hashCode() : 0);
    return hash;
  }

}

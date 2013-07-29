/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api.codec;

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
 * @author Pramod Immaneni <pramod@datatorrent.com>
 * @since 0.3.2
 */
@DefaultSerializer(KryoJdkSerializer.class)
public class KryoJdkContainer<T> implements Serializable
{
  private static final long serialVersionUID = 201306031549L;
  private T t;

  /**
   * <p>Constructor for KryoJdkContainer.</p>
   */
  public KryoJdkContainer()
  {
  }

  /**
   * <p>Constructor for KryoJdkContainer.</p>
   */
  public KryoJdkContainer(T t)
  {
    this.t = t;
  }

  /**
   * <p>setComponent.</p>
   */
  public void setComponent(T t)
  {
    this.t = t;
  }

  /**
   * <p>getComponent.</p>
   */
  public T getComponent()
  {
    return t;
  }

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 29 * hash + (this.t != null ? this.t.hashCode() : 0);
    return hash;
  }

}

/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.api;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;

/**
 * Base class for operator implementations that provides empty implementations
 * for all interface methods.
 *
 * @since 0.3.2
 */
public class BaseOperator implements Operator, Serializable
{
  private String name;

  /**
   * <p>Getter for the field
   * <code>name</code>.</p>
   */
  public String getName()
  {
    return name;
  }

  /**
   * <p>Setter for the field
   * <code>name</code>.</p>
   */
  public void setName(String name)
  {
    this.name = name;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void endWindow()
  {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void teardown()
  {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{name=" + name + '}';
  }

  /**
   * Create an instance of this operator class by invoking the copy
   * constructor. If the copy constructor does not exist, this method tries
   * to get a new instance using default constructor and then initializing all
   * the non final or non transient fields of the new instance with the
   * corresponding values from the deserialized operator. Users can override
   * this method to invoke non default constructor. They can override transferState
   * to just copy over the non final or non transient values from various fields
   * of the src object.
   *
   * @return an instance of the operator.
   * @throws ObjectStreamException
   */
  public Object readResolve() throws ObjectStreamException
  {
    try {
      Constructor<? extends BaseOperator> constructor = this.getClass().getConstructor(this.getClass());

      try {
        constructor.setAccessible(true);
      }
      catch (SecurityException ex) {
        logger.warn("Accessing copy constructor {} failed.", constructor, ex);
      }

      try {
        return constructor.newInstance(this);
      }
      catch (InstantiationException ex) {
        throw new RuntimeException("Instantiation using copy constructor failed!", ex);
      }
      catch (IllegalAccessException ex) {
        throw new RuntimeException("Instantiation using copy constructor failed!", ex);
      }
      catch (IllegalArgumentException ex) {
        throw new RuntimeException("Instantiation using copy constructor failed!", ex);
      }
      catch (InvocationTargetException ex) {
        throw new RuntimeException("Instantiation using copy constructor failed!", ex);
      }
    }
    catch (NoSuchMethodException snme) {
      logger.debug("No copy constructor detected, trying default constructor...");
      try {
        BaseOperator newInstance = this.getClass().newInstance();
        transferStateTo(newInstance);
        return newInstance;
      }
      catch (IllegalAccessException ex) {
        throw new RuntimeException("Deserialization using default constructor failed!", ex);
      }
      catch (InstantiationException ex) {
        throw new RuntimeException("Deserialization using default constructor failed!", ex);
      }
    }
  }

  /**
   * Copy over non final and non transient values from src to dest object.
   *
   * @param dest The object to which values are copied.
   * @param src The object from which values are copied.
   */
  public void transferStateTo(Object dest)
  {
  }

  private static final Logger logger = LoggerFactory.getLogger(BaseOperator.class);
  private static final long serialVersionUID = 201404140831L;
}

/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.codec.DefaultStatefulStreamCodec;
import com.datatorrent.api.StreamCodec;

/**
 *
 * Utilities for shared use in Stram components<p>
 * <br>
 */
public abstract class StramUtils
{
  @SuppressWarnings({"unchecked"})
  public static StreamCodec<Object> getSerdeInstance(String className)
  {
    if (className != null) {
      return newInstance(classForName(className, StreamCodec.class));
    }
    else {
      return new DefaultStatefulStreamCodec<Object>();
    }
  }

  public static <T> Class<? extends T> classForName(String className, Class<T> superClass)
  {
    try {
      //return Class.forName(className).asSubclass(superClass);
      return Thread.currentThread().getContextClassLoader().loadClass(className).asSubclass(superClass);
    }
    catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Class not found: " + className, e);
    }
  }

  public static <T> T newInstance(Class<T> clazz)
  {
    try {
      return clazz.newInstance();
    }
    catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to instantiate " + clazz, e);
    }
    catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate " + clazz, e);
    }
  }

}

/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.DefaultOperatorSerDe;
import com.malhartech.api.OperatorSerDe;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.SerDe;

/**
 *
 * Utilities for shared use in Stram components<p>
 * <br>
 */
public abstract class StramUtils {

  public static SerDe getSerdeInstance(String className) {
    if (className != null) {
      return newInstance(classForName(className, SerDe.class));
    } else {
      return new DefaultSerDe();
    }
  }

  public static <T> Class<? extends T> classForName(String className, Class<T> superClass) {
    try {
      //return Class.forName(className).asSubclass(superClass);
      return Thread.currentThread().getContextClassLoader().loadClass(className).asSubclass(superClass);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Class not found: " + className, e);
    }
  }

  public static <T> T newInstance(Class<T> clazz) {
    try {
      return clazz.newInstance();
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to instantiate " + clazz, e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate " + clazz, e);
    }
  }

  public static OperatorSerDe getNodeSerDe(String className) {
    if (className != null) {
      return newInstance(classForName(className, OperatorSerDe.class));
    }
    return new DefaultOperatorSerDe();
  }

}

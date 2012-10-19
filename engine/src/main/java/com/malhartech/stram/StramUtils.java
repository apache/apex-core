/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.Operator;
import com.malhartech.dag.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.commons.beanutils.BeanUtils;

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

  /**
   * Instantiate node from configuration.
   *
   * @param nodeClass
   * @param properties
   */
  public static Operator initNode(Class<? extends Operator> nodeClass, String id, Map<String, String> properties)
  {
    try {
      Constructor<? extends Operator> c = nodeClass.getConstructor();
      Operator node = c.newInstance();
      // populate custom properties
      BeanUtils.populate(node, properties);
      return node;
    }
    catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (SecurityException e) {
      throw new IllegalArgumentException("Error creating instance of class: " + nodeClass, e);
    }
    catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Constructor not found: " + nodeClass, e);
    }
    catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate: " + nodeClass, e);
    }
  }

  public static OperatorSerDe getNodeSerDe(String className) {
    if (className != null) {
      return newInstance(classForName(className, OperatorSerDe.class));
    }
    return new DefaultModuleSerDe();
  }

}

/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;

import com.malhartech.dag.DefaultNodeSerDe;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.Node;
import com.malhartech.dag.NodeSerDe;
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

  public static final <T> Class<? extends T> classForName(String className, Class<T> superClass) {
    try {
      //return Class.forName(className).asSubclass(superClass);
      return Thread.currentThread().getContextClassLoader().loadClass(className).asSubclass(superClass);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Class not found: " + className, e);
    }
  }

  public static final <T> T newInstance(Class<T> clazz) {
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
  public static Node initNode(Class<? extends Node> nodeClass, String id, Map<String, String> properties)
  {
    try {
      Constructor<? extends Node> c = nodeClass.getConstructor();
      Node node = c.newInstance();
      // populate custom properties
      BeanUtils.populate(node, properties);
      internalSetupNode(node, id);
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
      throw new IllegalArgumentException("Constructor with NodeContext not found: " + nodeClass, e);
    }
    catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate: " + nodeClass, e);
    }
  }

  /**
   * Initialize internal field(s) on node base class.
   * To be called along with {@link Node#setup}
   * @param node
   * @param id
   */
  public static void internalSetupNode(Node node, String id) {
    if (node instanceof com.malhartech.dag.AbstractNode) {
      // TODO: replace this with internal initialization interface on nodes
      try {
        Field f = com.malhartech.dag.AbstractNode.class.getDeclaredField("id");
        f.setAccessible(true);
        f.set(node, id);
      } catch (Exception e) {
        throw new RuntimeException("Set id failed for node " + node + ", id=" + id, e);
      }
    }
  }

  public static NodeSerDe getNodeSerDe(String className) {
    if (className != null) {
      return newInstance(classForName(className, NodeSerDe.class));
    }
    return new DefaultNodeSerDe();
  }

}

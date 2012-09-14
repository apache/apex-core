/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.AbstractInputNode;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.DefaultNodeSerDe;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.Node;
import com.malhartech.dag.NodeSerDe;
import com.malhartech.dag.SerDe;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.logging.Level;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Utilities for shared use in Stram components<p>
 * <br>
 */
public abstract class StramUtils {

  private static final Logger logger = LoggerFactory.getLogger(StramUtils.class);

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
    try {
      Field f = node instanceof AbstractNode? AbstractNode.class.getDeclaredField("id"): node instanceof AbstractInputNode? AbstractInputNode.class.getDeclaredField("id"): null;
      f.setAccessible(true);
      f.set(node, id);
    }
    catch (NullPointerException npe) {
      logger.debug(npe.getLocalizedMessage());
    }
    catch (Exception se) {
      logger.debug(se.getLocalizedMessage());
      throw new RuntimeException("Could not set id field of the node", se);
    }
  }

  public static NodeSerDe getNodeSerDe(String className) {
    if (className != null) {
      return newInstance(classForName(className, NodeSerDe.class));
    }
    return new DefaultNodeSerDe();
  }

}

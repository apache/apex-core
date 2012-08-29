/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;

import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.SerDe;
import com.malhartech.stram.conf.TopologyBuilder;

/**
 *
 * Utilities for shared use in Stram components<p>
 * <br>
 */
public abstract class StramUtils {

  public static SerDe getSerdeInstance(Map<String, String> streamProps) {
    String className = streamProps.get(TopologyBuilder.STREAM_SERDE_CLASSNAME);
    if (className != null) {
      return newInstance(classForName(className, SerDe.class));
    } else {
      return new DefaultSerDe();
    }
  }

  public static final <T> Class<? extends T> classForName(String className, Class<T> superClass) {
    try {
      return Class.forName(className).asSubclass(superClass);
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
   * Instantiate node from configuration. (happens in the child container, not the stram master process.)
   *
   * @param nodeConf
   * @param conf
   */
  public static AbstractNode initNode(String className, Map<String, String> properties)
  {      
    try {
      Class<? extends AbstractNode> nodeClass = classForName(className, AbstractNode.class);
      Constructor<? extends AbstractNode> c = nodeClass.getConstructor();
      AbstractNode node = c.newInstance();
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
      throw new IllegalArgumentException("Error creating instance of class: " + className, e);
    }
    catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Constructor with NodeContext not found: " + className, e);
    }
    catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate: " + className, e);
    }
  }

}

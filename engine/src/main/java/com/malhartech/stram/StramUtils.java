/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;

import com.malhartech.dag.AbstractInputModule;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.DefaultModuleSerDe;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.Module;
import com.malhartech.dag.ModuleSerDe;
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

  /**
   * Instantiate node from configuration.
   *
   * @param nodeClass
   * @param properties
   */
  public static Module initNode(Class<? extends Module> nodeClass, String id, Map<String, String> properties)
  {
    try {
      Constructor<? extends Module> c = nodeClass.getConstructor();
      Module node = c.newInstance();
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
   * To be called along with {@link Module#setup}
   * @param node
   * @param id
   */
  public static void internalSetupNode(Module node, String id) {
    // TODO: what we really need is a common node interface for internal setup
    if (node instanceof AbstractModule) {
      ((AbstractModule)node).setId(id);
    } else if (node instanceof AbstractInputModule) {
      ((AbstractInputModule)node).setId(id);
    }
  }

  public static ModuleSerDe getNodeSerDe(String className) {
    if (className != null) {
      return newInstance(classForName(className, ModuleSerDe.class));
    }
    return new DefaultModuleSerDe();
  }

}

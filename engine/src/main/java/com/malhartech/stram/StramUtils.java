/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.Node;
import com.malhartech.dag.SerDe;
import com.malhartech.stram.conf.TopologyBuilder;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * Utilities for shared use in Stram components<p>
 * <br>
 */
public abstract class StramUtils {

  public static SerDe getSerdeInstance(Map<String, String> streamProps) {
    String className = streamProps.get(TopologyBuilder.STREAM_SERDE_CLASSNAME);
    if (className != null) {
      try {
        Class<? extends SerDe> serdeClass = Class.forName(className).asSubclass(SerDe.class);
        return serdeClass.newInstance();
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("SerDe class not found: " + className, e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Failed to instantiate SerDe", e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Failed to instantiate SerDe", e);
      }
    } else {
      return new DefaultSerDe();
    }
  }

  /**
   * Instantiate node from configuration. (happens in the child container, not the stram master process.)
   *
   * @param nodeConf
   * @param conf
   */
  public static Node initNode(NodePConf nodeConf, Configuration conf)
  {
    try {
      Class<? extends Node> nodeClass = Class.forName(nodeConf.getDnodeClassName()).asSubclass(Node.class);
      Constructor<? extends Node> c = nodeClass.getConstructor();
      Node node = c.newInstance();
      // populate custom properties
      BeanUtils.populate(node, nodeConf.getProperties());
      return node;
    }
    catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Node class not found: " + nodeConf.getDnodeClassName(), e);
    }
    catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (SecurityException e) {
      throw new IllegalArgumentException("Error creating instance of class: " + nodeConf.getDnodeClassName(), e);
    }
    catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Constructor with NodeContext not found: " + nodeConf.getDnodeClassName(), e);
    }
    catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate: " + nodeConf.getDnodeClassName(), e);
    }
  }

}

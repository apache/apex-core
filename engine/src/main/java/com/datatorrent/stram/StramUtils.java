/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;


import java.util.Map;

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;

/**
 *
 * Utilities for shared use in Stram components<p>
 * <br>
 *
 * @since 0.3.2
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

  public static abstract class YarnContainerMain
  {
    static {
      // set system properties so they can be used in logger configuration
      Map<String, String> envs = System.getenv();
      String containerIdString = envs.get(Environment.CONTAINER_ID.name());
      if (containerIdString != null) {
        System.setProperty(StreamingApplication.DT_PREFIX + "cid", containerIdString);
      }

      System.setProperty("hadoop.log.file", "dt.log");
      if (envs.get("CDH_YARN_HOME") != null) {
        // map logging properties to what CHD expects out of the box
        String[] keys = new String[] { "log.dir", "log.file", "root.logger" };
        for (String key : keys) {
          String v = System.getProperty("hadoop." + key);
          if (v != null) {
            System.setProperty(key, v);
          }
        }
      }
    }
  }

}

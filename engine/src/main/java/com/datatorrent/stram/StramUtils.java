/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram;


import java.util.Map;

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.log4j.DTLoggerFactory;

import com.datatorrent.api.StreamingApplication;

/**
 *
 * Utilities for shared use in Stram components<p>
 * <br>
 *
 * @since 0.3.2
 */
public abstract class StramUtils
{
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

  public static Class<?> classForName(String className)
  {
    try {
      return Thread.currentThread().getContextClassLoader().loadClass(className);
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
      DTLoggerFactory.getInstance().initialize();
    }
  }

}

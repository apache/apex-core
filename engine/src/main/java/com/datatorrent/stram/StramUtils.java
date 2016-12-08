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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.util.LoggerUtil;

/**
 *
 * Utilities for shared use in Stram components<p>
 * <br>
 *
 * @since 0.3.2
 */
public abstract class StramUtils
{
  private static final Logger LOG = LoggerFactory.getLogger(StramUtils.class);
  public static final String DT_LOGGERS_LEVEL = "dt.loggers.level";

  public static <T> Class<? extends T> classForName(String className, Class<T> superClass)
  {
    try {
      //return Class.forName(className).asSubclass(superClass);
      return Thread.currentThread().getContextClassLoader().loadClass(className).asSubclass(superClass);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Class not found: " + className, e);
    }
  }

  public static <T> T newInstance(Class<T> clazz)
  {
    try {
      return clazz.newInstance();
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to instantiate " + clazz, e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate " + clazz, e);
    }
  }

  public abstract static class YarnContainerMain
  {
    static {
      // set system properties so they can be used in logger configuration
      Map<String, String> envs = System.getenv();
      String containerIdString = envs.get(Environment.CONTAINER_ID.name());
      if (containerIdString != null) {
        System.setProperty(StreamingApplication.DT_PREFIX + "cid", containerIdString);
      }

      System.setProperty("hadoop.log.file", "apex.log");
      if (envs.get("CDH_YARN_HOME") != null) {
        // map logging properties to what CHD expects out of the box
        String[] keys = new String[]{"log.dir", "log.file", "root.logger"};
        for (String key : keys) {
          String v = System.getProperty("hadoop." + key);
          if (v != null) {
            System.setProperty(key, v);
          }
        }
      }

      String loggersLevel = System.getProperty(DT_LOGGERS_LEVEL);
      if (!Strings.isNullOrEmpty(loggersLevel)) {
        Map<String, String> targetChanges = Maps.newHashMap();
        String[] targets = loggersLevel.split(",");
        for (String target : targets) {
          String[] parts = target.split(":");
          targetChanges.put(parts[0], parts[1]);
        }
        LoggerUtil.changeLoggersLevel(targetChanges);
      }

    }
  }

  public static JSONObject getStackTrace()
  {
    Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();

    JSONObject jsonObject = new JSONObject();
    JSONArray jsonArray = new JSONArray();

    for (Map.Entry<Thread, StackTraceElement[]> elements : stackTraces.entrySet()) {

      JSONObject jsonThread = new JSONObject();

      Thread thread = elements.getKey();

      try {

        jsonThread.put("name", thread.getName());
        jsonThread.put("state", thread.getState());
        jsonThread.put("id", thread.getId());

        JSONArray stackTraceElements = new JSONArray();

        for (StackTraceElement stackTraceElement : elements.getValue()) {

          stackTraceElements.put(stackTraceElement.toString());
        }

        jsonThread.put("stackTraceElements", stackTraceElements);

        jsonArray.put(jsonThread);
      } catch (Exception ex) {
        LOG.warn("Getting stack trace for the thread " + thread.getName() + " failed.");
        continue;
      }
    }

    try {
      jsonObject.put("threads", jsonArray);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }

    return jsonObject;
  }


  public static <T> T getValueWithDefault(Attribute.AttributeMap map, Attribute<T> key)
  {
    T value = map.get(key);
    if (value == null) {
      value = key.defaultValue;
    }
    return value;
  }

  public static List<ApplicationReport> getApexApplicationList(YarnClient yarnClient) throws IOException, YarnException
  {
    return yarnClient.getApplications(Sets.newHashSet(StramClient.YARN_APPLICATION_TYPE, StramClient.YARN_APPLICATION_TYPE_DEPRECATED));
  }

}

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
package com.datatorrent.common.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

/**
 * The default implementation for {@link Context.ContainerOptConfigurator}
 * <br>
 * This aggregates the heap size configurations for Xmx, Xms, Xss. If Xmx is not set for an operator, it is set to 75% of the operator memory <br>
 * For other JVM options it assumes that they are all set same for all operators deployed in container. If JVM options are set on only one operator in the container,then those configurations will be applied to all the operators deployed in the container
 *
 * @since 2.0.0
 */
public class BasicContainerOptConfigurator implements Context.ContainerOptConfigurator
{
  private static final Logger LOG = LoggerFactory.getLogger(BasicContainerOptConfigurator.class);
  private static final String XMX = "-Xmx";
  private static final String XMS = "-Xms";
  private static final String XSS = "-Xss";
  private static final String[] OPT_LIST = new String[]{XMS, XMX, XSS};
  private static final String GENERIC = "Generic";
  private static final int GB_TO_B = 1024 * 1024 * 1024;
  private static final int MB_TO_B = 1024 * 1024;
  private static final int KB_TO_B = 1024;

  @Override
  public String getJVMOptions(List<DAG.OperatorMeta> operatorMetaList)
  {
    Set<String> genericProperties = null;
    long xmx = 0;
    long xms = 0;
    long xss = 0;
    List<Map<String, Object>> jvmOptsList = Lists.newArrayList();
    for (DAG.OperatorMeta operatorMeta : operatorMetaList) {
      Map<String, Object> operatorMap = parseJvmOpts(operatorMeta.getValue(Context.OperatorContext.JVM_OPTIONS), operatorMeta.getValue(Context.OperatorContext.MEMORY_MB));
      LOG.info("property map for operator {}", operatorMap);
      Set<String> operatorPropertySet = (Set<String>)operatorMap.get(GENERIC);
      if (genericProperties == null) {
        genericProperties = operatorPropertySet;
      } else {
        if (operatorPropertySet != null && !genericProperties.equals(operatorPropertySet)) {
          throw new AssertionError("Properties don't match: " + genericProperties + " " + operatorPropertySet);
        }
      }
      jvmOptsList.add(operatorMap);
    }
    for (Map<String, Object> map : jvmOptsList) {
      String value;
      if (map.containsKey(XMX)) {
        value = (String)map.get(XMX);
        xmx += getOptValue(value);
      }
      if (map.containsKey(XMS)) {
        value = (String)map.get(XMS);
        xms += getOptValue(value);
      }
      if (map.containsKey(XSS)) {
        value = (String)map.get(XSS);
        xss += getOptValue(value);
      }
    }
    StringBuilder builder = new StringBuilder(" ");
    builder.append(XMX).append(xmx);
    if (xms != 0) {
      builder.append(" ").append(XMS).append(xms);
    }
    if (xss != 0) {
      builder.append(" ").append(XSS).append(xss);
    }
    if (genericProperties != null) {
      for (String property : genericProperties) {
        builder.append(" ").append(property);
      }
    }
    return builder.toString();
  }

  private long getOptValue(String value)
  {
    long result;
    if (value.endsWith("g") || value.endsWith("G")) {
      result = Long.valueOf(value.substring(0, value.length() - 1)) * GB_TO_B;
    } else if (value.endsWith("m") || value.endsWith("M")) {
      result = Long.valueOf(value.substring(0, value.length() - 1)) * MB_TO_B;
    } else if (value.endsWith("k") || value.endsWith("K")) {
      result = Long.valueOf(value.substring(0, value.length() - 1)) * KB_TO_B;
    } else {
      result = Long.valueOf(value);
    }
    return result;
  }

  private Map<String, Object> parseJvmOpts(String jvmOpts, int memory)
  {
    Set<String> currentProperties = null;
    Map<String, Object> map = Maps.newHashMap();
    if (jvmOpts != null && jvmOpts.length() > 1) {
      String[] splits = jvmOpts.split("(\\s+)");
      boolean foundProperty;
      for (String split : splits) {
        foundProperty = false;
        for (String opt : OPT_LIST) {
          if (split.startsWith(opt)) {
            foundProperty = true;
            map.put(opt, split.substring(opt.length()));
            break;
          }
        }
        if (!foundProperty) {
          if (currentProperties == null) {
            currentProperties = Sets.newHashSet();
          }
          currentProperties.add(split);
        }
      }
    }
    if (map.get(XMX) == null) {
      int memoryOverhead = memory / 4;
      int heapSize = memory - memoryOverhead;
      if (memoryOverhead > 1024) {
        heapSize = memory - 1024;
      }
      map.put(XMX, heapSize + "m");
    }

    map.put(GENERIC, currentProperties);
    return map;
  }
}

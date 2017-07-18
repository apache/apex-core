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
package org.apache.apex.engine.plugin.loaders;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.plugin.Plugin;
import org.apache.apex.engine.api.plugin.PluginLocator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.StramUtils;

/**
 * @since 3.6.0
 */
public class PropertyBasedPluginLocator<T extends Plugin> implements PluginLocator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PropertyBasedPluginLocator.class);
  private final Class<T> klass;
  private final String propertyName;

  public PropertyBasedPluginLocator(Class<T> klass, String propertyName)
  {
    this.klass = klass;
    this.propertyName = propertyName;
  }

  @Override
  public Set<T> discoverPlugins(Configuration conf)
  {
    Set<T> detectedPlugins = new LinkedHashSet<>();
    String classNamesStr = conf.get(this.propertyName);
    if (StringUtils.isBlank(classNamesStr)) {
      return detectedPlugins;
    }

    Set<String> classNames = new LinkedHashSet<>();
    Collections.addAll(classNames, classNamesStr.split(","));
    for (String className : classNames) {
      try {
        Class<?> plugin = StramUtils.classForName(className, Object.class);
        if (klass.isAssignableFrom(plugin)) {
          detectedPlugins.add(StramUtils.newInstance(plugin.asSubclass(klass)));
        } else {
          LOG.info("Skipping loading {} incompatible with {}", className, klass);
        }
      } catch (IllegalArgumentException e) {
        LOG.warn("Could not load plugin {}", className, e);
      }
    }
    return detectedPlugins;
  }
}

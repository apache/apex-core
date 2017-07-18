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
import org.apache.hadoop.conf.Configuration;

/**
 * @since 3.6.0
 */
public class ChainedPluginLocator<T extends Plugin> implements PluginLocator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(ChainedPluginLocator.class);

  private final Set<PluginLocator<T>> locators;

  public ChainedPluginLocator(PluginLocator<T>... locators)
  {
    Collections.addAll(this.locators = new LinkedHashSet(locators.length), locators);
  }

  private static <T extends Plugin> Set<T> merge(Set<T> to, Set<T> from)
  {
    if (from == null || from.isEmpty()) {
      return to;
    }

    if (to == null || to.isEmpty()) {
      if (from instanceof LinkedHashSet) {
        return from;
      } else {
        to = new LinkedHashSet<>(from.size());
      }
    }
    to.addAll(from);
    return to;
  }

  @Override
  public Set<T> discoverPlugins(Configuration conf)
  {
    Set<T> plugins = Collections.emptySet();

    for (PluginLocator<T> locator : locators) {
      Set<T> currentPlugins = locator.discoverPlugins(conf);
      if (!currentPlugins.isEmpty()) {
        LOG.info("Plugin locator {} detected {} plugins", locator.getClass().getSimpleName(), currentPlugins.size());
        plugins = merge(plugins, currentPlugins);
      }
    }

    return plugins;
  }
}

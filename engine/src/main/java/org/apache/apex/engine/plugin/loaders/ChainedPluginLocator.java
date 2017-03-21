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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.api.PluginLocator;
import org.apache.hadoop.conf.Configuration;

public class ChainedPluginLocator<T> implements PluginLocator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(ChainedPluginLocator.class);
  List<PluginLocator> locators = new ArrayList<>();

  public ChainedPluginLocator(PluginLocator<T>... locators)
  {
    for (PluginLocator locator : locators) {
      this.locators.add(locator);
    }
  }

  @Override
  public Collection<T> discoverPlugins(Configuration conf)
  {
    List<T> plugins = new ArrayList<>();

    for (PluginLocator<T> locator : locators) {
      Collection<T> currentPlugins = locator.discoverPlugins(conf);
      if (currentPlugins != null) {
        LOG.info("Loader {} detected {} plugins ", locator.getClass().getName(), currentPlugins.size());
        plugins.addAll(currentPlugins);
      }
    }

    return plugins;
  }
}

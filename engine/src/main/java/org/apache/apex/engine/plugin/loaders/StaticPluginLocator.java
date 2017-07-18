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
public class StaticPluginLocator<T extends Plugin> implements PluginLocator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(StaticPluginLocator.class);

  private final Set<T> plugins;

  public StaticPluginLocator(T... plugins)
  {
    this.plugins = new LinkedHashSet<>(plugins.length);
    Collections.addAll(this.plugins, plugins);
  }

  @Override
  public Set<T> discoverPlugins(Configuration conf)
  {
    return plugins;
  }
}

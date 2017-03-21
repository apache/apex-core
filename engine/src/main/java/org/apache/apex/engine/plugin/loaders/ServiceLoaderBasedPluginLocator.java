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
import java.util.ServiceLoader;

import org.apache.apex.engine.api.PluginLocator;
import org.apache.hadoop.conf.Configuration;

public class ServiceLoaderBasedPluginLocator<T> implements PluginLocator<T>
{
  private final Class<T> klass;

  public ServiceLoaderBasedPluginLocator(Class<T> klass)
  {
    this.klass = klass;
  }

  @Override
  public Collection<T> discoverPlugins(Configuration conf)
  {
    List<T> discovered = new ArrayList<>();
    ServiceLoader<T> loader = ServiceLoader.load(this.klass);
    for (T plugin : loader) {
      discovered.add(plugin);
    }
    return discovered;
  }
}

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
package com.datatorrent.stram.plan.logical;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import org.apache.apex.api.DAGSetupPlugin;
import org.apache.apex.engine.plugin.loaders.PropertyBasedPluginLocator;
import org.apache.hadoop.conf.Configuration;

import static org.slf4j.LoggerFactory.getLogger;

public class DAGSetupPluginManager
{
  private static final Logger LOG = getLogger(DAGSetupPluginManager.class);

  private final transient List<DAGSetupPlugin> plugins = new ArrayList<>();
  private Configuration conf;

  public static final String DAGSETUP_PLUGINS_CONF_KEY = "apex.plugin.dag.setup";
  private DAGSetupPlugin.DAGSetupPluginContext contex;

  private void loadVisitors(Configuration conf)
  {
    this.conf = conf;
    if (!plugins.isEmpty()) {
      return;
    }

    PropertyBasedPluginLocator<DAGSetupPlugin> locator = new PropertyBasedPluginLocator<>(DAGSetupPlugin.class, DAGSETUP_PLUGINS_CONF_KEY);
    this.plugins.addAll(locator.discoverPlugins(conf));
  }

  public void setup(DAGSetupPlugin.DAGSetupPluginContext context)
  {
    this.contex = context;
    for (DAGSetupPlugin plugin : plugins) {
      plugin.setup(context);
    }
  }

  public enum DispatchType
  {
    SETUP,
    PRE_POPULATE,
    POST_POPULATE,
    PRE_CONFIGURE,
    POST_CONFIGURE,
    PRE_VALIDATE,
    POST_VALIDATE,
    TEARDOWN
  }

  public void dispatch(DispatchType type, DAGSetupPlugin.DAGSetupPluginContext context)
  {
    for (DAGSetupPlugin plugin : plugins) {
      switch (type) {
        case SETUP:
          plugin.setup(context);
          break;
        case PRE_POPULATE:
          plugin.prePopulateDAG();
          break;
        case POST_POPULATE:
          plugin.postPopulateDAG();
          break;
        case PRE_CONFIGURE:
          plugin.preConfigureDAG();
          break;
        case POST_CONFIGURE:
          plugin.postValidateDAG();
          break;
        case PRE_VALIDATE:
          plugin.preValidateDAG();
          break;
        case POST_VALIDATE:
          plugin.postValidateDAG();
          break;
        case TEARDOWN:
          plugin.teardown();
          break;
        default:
          throw new UnsupportedOperationException("Not implemented ");
      }
    }
  }

  public static synchronized DAGSetupPluginManager getInstance(Configuration conf)
  {
    DAGSetupPluginManager manager = new DAGSetupPluginManager();
    manager.loadVisitors(conf);
    return manager;
  }
}

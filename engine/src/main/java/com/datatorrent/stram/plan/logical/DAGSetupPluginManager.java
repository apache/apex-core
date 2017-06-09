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
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;

import org.apache.apex.api.plugin.DAGSetupEvent;
import org.apache.apex.api.plugin.DAGSetupPlugin;
import org.apache.apex.api.plugin.Plugin.EventHandler;
import org.apache.apex.engine.plugin.loaders.PropertyBasedPluginLocator;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @since 3.6.0
 */
public class DAGSetupPluginManager
{
  private static final Logger LOG = getLogger(DAGSetupPluginManager.class);

  private final transient List<DAGSetupPlugin> plugins = new ArrayList<>();
  private Configuration conf;

  public static final String DAGSETUP_PLUGINS_CONF_KEY = "apex.plugin.dag.setup";

  private final Table<DAGSetupEvent.Type, DAGSetupPlugin, EventHandler<DAGSetupEvent>> table = HashBasedTable.create();

  private DAGSetupPluginManager(Configuration conf)
  {
    this.conf = conf;
  }

  private void loadVisitors()
  {
    if (!plugins.isEmpty()) {
      return;
    }

    PropertyBasedPluginLocator<DAGSetupPlugin> locator = new PropertyBasedPluginLocator<>(DAGSetupPlugin.class, DAGSETUP_PLUGINS_CONF_KEY);
    this.plugins.addAll(locator.discoverPlugins(conf));
  }

  private class DefaultDAGSetupPluginContext implements DAGSetupPlugin.Context<DAGSetupEvent>
  {
    private final DAG dag;
    private final Configuration conf;
    private DAGSetupPlugin plugin;

    public DefaultDAGSetupPluginContext(DAG dag, Configuration conf, DAGSetupPlugin plugin)
    {
      this.dag = dag;
      this.conf = conf;
      this.plugin = plugin;
    }

    @Override
    public void register(DAGSetupEvent.Type type, EventHandler<DAGSetupEvent> handler)
    {
      table.put(type, plugin, handler);
    }

    public DAG getDAG()
    {
      return dag;
    }

    public Configuration getConfiguration()
    {
      return conf;
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void sendMetrics(Collection<String> metricNames)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public void setup(DAG dag)
  {
    loadVisitors();
    for (DAGSetupPlugin plugin : plugins) {
      DAGSetupPlugin.Context context = new DefaultDAGSetupPluginContext(dag, conf, plugin);
      plugin.setup(context);
    }
  }

  public void teardown()
  {
    for (DAGSetupPlugin plugin : plugins) {
      plugin.teardown();
    }
  }

  public void dispatch(DAGSetupEvent event)
  {
    for (EventHandler<DAGSetupEvent> handler : table.row(event.getType()).values()) {
      try {
        handler.handle(event);
      } catch (RuntimeException e) {
        LOG.warn("Event {} caused an exception in {} handler", event, handler, e);
      }
    }
  }

  public static synchronized DAGSetupPluginManager getInstance(Configuration conf)
  {
    DAGSetupPluginManager manager = new DAGSetupPluginManager(conf);
    return manager;
  }
}

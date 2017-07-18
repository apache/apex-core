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
package org.apache.apex.engine.plugin;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.plugin.Event;
import org.apache.apex.api.plugin.Plugin;
import org.apache.apex.api.plugin.Plugin.EventHandler;
import org.apache.apex.engine.api.plugin.DAGExecutionEvent;
import org.apache.apex.engine.api.plugin.DAGExecutionPlugin;
import org.apache.apex.engine.api.plugin.PluginLocator;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import com.datatorrent.api.DAG;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.webapp.AppInfo;

/**
 * A default implementation for ApexPluginDispatcher. It handles common tasks, such as handler
 * registrations. Actual dispatching is left for classes extending from it.
 *
 * @since 3.6.0
 */
public abstract class AbstractApexPluginDispatcher extends AbstractService implements ApexPluginDispatcher
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractApexPluginDispatcher.class);
  protected final Collection<DAGExecutionPlugin> plugins = Lists.newArrayList();
  protected final StramAppContext appContext;
  protected final StreamingContainerManager dmgr;
  private final PluginLocator<DAGExecutionPlugin> locator;
  private final AppInfo.AppStats stats;
  protected Configuration launchConfig;
  protected FileContext fileContext;
  protected final Table<DAGExecutionEvent.Type, DAGExecutionPlugin, EventHandler<DAGExecutionEvent>> table = HashBasedTable.create();
  private volatile DAG clonedDAG = null;

  protected AbstractApexPluginDispatcher(String name, PluginLocator<DAGExecutionPlugin> locator, StramAppContext context, StreamingContainerManager dmgr, AppInfo.AppStats stats)
  {
    super(name);
    this.locator = locator;
    this.appContext = context;
    this.dmgr = dmgr;
    this.stats = stats;
    LOG.debug("Creating Plugin Dispatcher service {}", name);
  }

  private Configuration readLaunchConfiguration() throws IOException
  {
    Path appPath = new Path(appContext.getApplicationPath());
    Path  configFilePath = new Path(appPath, LogicalPlan.LAUNCH_CONFIG_FILE_NAME);
    try {
      LOG.debug("Reading launch configuration file ");
      URI uri = appPath.toUri();
      Configuration config = new YarnConfiguration();
      fileContext = uri.getScheme() == null ? FileContext.getFileContext(config) : FileContext.getFileContext(uri, config);
      FSDataInputStream is = fileContext.open(configFilePath);
      config.addResource(is);
      LOG.debug("Read launch configuration");
      return config;
    } catch (FileNotFoundException ex) {
      LOG.warn("Configuration file not found {}", configFilePath);
      return new Configuration();
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    super.serviceInit(conf);
    this.launchConfig = readLaunchConfiguration();
    if (locator != null) {
      Collection<DAGExecutionPlugin> plugins = locator.discoverPlugins(this.launchConfig);
      if (plugins != null) {
        this.plugins.addAll(plugins);
        for (DAGExecutionPlugin plugin : plugins) {
          LOG.info("Detected plugin {}", plugin);
        }
      }
    }

    for (DAGExecutionPlugin plugin : plugins) {
      plugin.setup(new PluginManagerImpl(plugin));
    }
  }

  @Override
  protected void serviceStop() throws Exception
  {
    for (DAGExecutionPlugin plugin : plugins) {
      try {
        plugin.teardown();
      } catch (Exception e) {
        LOG.warn("Exception during {} teardown", plugin, e);
      }
    }
    super.serviceStop();
  }

  public void register(DAGExecutionEvent.Type eventType, Plugin.EventHandler<DAGExecutionEvent> handler, DAGExecutionPlugin owner)
  {
    synchronized (table) {
      table.put(eventType, owner, handler);
    }
  }

  /**
   * A wrapper PluginManager to track registration from a plugin. with this plugin
   * don't need to pass explicit owner argument during registration.
   */
  private class PluginManagerImpl extends AbstractDAGExecutionPluginContext<DAGExecutionEvent>
  {
    private final DAGExecutionPlugin owner;

    PluginManagerImpl(DAGExecutionPlugin plugin)
    {
      super(appContext, dmgr, stats, launchConfig);
      this.owner = plugin;
    }

    @Override
    public void register(DAGExecutionEvent.Type type, EventHandler<DAGExecutionEvent> handler)
    {
      AbstractApexPluginDispatcher.this.register(type, handler, owner);
    }

    @Override
    public DAG getDAG()
    {
      return clonedDAG;
    }
  }

  /**
   * Dispatch events to plugins.
   * @param event The dag execution event
   */
  protected abstract void dispatchExecutionEvent(DAGExecutionEvent event);

  @Override
  public void dispatch(Event event)
  {
    if (event.getType() == ApexPluginDispatcher.DAG_CHANGE) {
      clonedDAG = SerializationUtils.clone(((DAGChangeEvent)event).dag);
    } else if (!plugins.isEmpty() && (event instanceof DAGExecutionEvent)) {
      dispatchExecutionEvent((DAGExecutionEvent)event);
    }
  }
}

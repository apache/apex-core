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
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.api.DAGExecutionPlugin;
import org.apache.apex.engine.api.DAGExecutionPluginContext.Handler;
import org.apache.apex.engine.api.DAGExecutionPluginContext.RegistrationType;
import org.apache.apex.engine.api.PluginLocator;
import org.apache.commons.digester.plugins.PluginContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.Lists;

import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.webapp.AppInfo;

/**
 * A default implementation for ApexPluginDispatcher. It handler common tasks such as per handler
 * registration. actual dispatching is left for classes extending from it.
 */
public abstract class ApexPluginManager extends AbstractService
{
  private static final Logger LOG = LoggerFactory.getLogger(ApexPluginManager.class);
  protected final Collection<DAGExecutionPlugin> plugins = Lists.newArrayList();
  protected final StramAppContext appContext;
  protected final StreamingContainerManager dmgr;
  private final PluginLocator locator;
  private final AppInfo.AppStats stats;
  protected Configuration launchConfig;
  protected FileContext fileContext;
  protected final Map<DAGExecutionPlugin, PluginInfo> pluginInfoMap = new HashMap<>();
  protected PluginContext pluginContext;

  public ApexPluginManager(PluginLocator locator, StramAppContext context, StreamingContainerManager dmgr, AppInfo.AppStats stats)
  {
    super(ApexPluginManager.class.getName());
    this.locator = locator;
    this.appContext = context;
    this.dmgr = dmgr;
    this.stats = stats;
    LOG.debug("Creating apex service ");
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
      plugin.teardown();
    }
    super.serviceStop();
  }

  /**
   * Keeps information about plugin and its registrations. Dispatcher use this
   * information while delivering events to plugin.
   */
  class PluginInfo
  {
    private final DAGExecutionPlugin plugin;
    private final Map<RegistrationType<?>, Handler<?>> registrationMap = new HashMap<>();

    <T> void put(RegistrationType<T> registrationType, Handler<T> handler)
    {
      registrationMap.put(registrationType, handler);
    }

    <T> Handler<T> get(RegistrationType<T> registrationType)
    {
      return (Handler<T>)registrationMap.get(registrationType);
    }

    public PluginInfo(DAGExecutionPlugin plugin)
    {
      this.plugin = plugin;
    }

    public DAGExecutionPlugin getPlugin()
    {
      return plugin;
    }
  }

  PluginInfo getPluginInfo(DAGExecutionPlugin plugin)
  {
    PluginInfo pInfo = pluginInfoMap.get(plugin);
    if (pInfo == null) {
      pInfo = new PluginInfo(plugin);
      pluginInfoMap.put(plugin, pInfo);
    }
    return pInfo;
  }

  public <T> void register(RegistrationType<T> type, Handler<T> handler, DAGExecutionPlugin owner)
  {
    PluginInfo pInfo = getPluginInfo(owner);
    pInfo.put(type, handler);
  }

  /**
   * A wrapper PluginManager to track registration from a plugin. with this plugin
   * don't need to pass explicit owner argument during registration.
   */
  class PluginManagerImpl extends AbstractDAGExecutionPluginContext
  {
    private final DAGExecutionPlugin owner;

    PluginManagerImpl(DAGExecutionPlugin plugin)
    {
      super(appContext, dmgr, stats, launchConfig);
      this.owner = plugin;
    }

    @Override
    public <T> void register(RegistrationType<T> type, Handler<T> handler)
    {
      ApexPluginManager.this.register(type, handler, owner);
    }
  }
}

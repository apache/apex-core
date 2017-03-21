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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.engine.api.DAGExecutionPlugin;
import org.apache.apex.engine.plugin.loaders.ChainedPluginLocator;
import org.apache.apex.engine.plugin.loaders.ServiceLoaderBasedPluginLocator;
import org.apache.apex.engine.plugin.loaders.StaticPluginLocator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.support.StramTestSupport;

import static org.apache.apex.engine.api.DAGExecutionPluginContext.COMMIT_EVENT;
import static org.apache.apex.engine.api.DAGExecutionPluginContext.HEARTBEAT;
import static org.apache.apex.engine.api.DAGExecutionPluginContext.STRAM_EVENT;

public class PluginTests
{

  private static final Configuration conf = new Configuration();

  @Test
  public void testStaticPluginLoader()
  {
    DAGExecutionPlugin plugin1 = new NoOpPlugin();
    DAGExecutionPlugin plugin2 = new DebugPlugin();

    StaticPluginLocator<DAGExecutionPlugin> locator1 = new StaticPluginLocator<>(plugin1);
    StaticPluginLocator<DAGExecutionPlugin> locator2 = new StaticPluginLocator<>(plugin2);

    Collection<DAGExecutionPlugin> discovered1 = locator1.discoverPlugins(conf);
    Assert.assertEquals("Number of plugins discovered ", 1, discovered1.size());
    Assert.assertEquals("Type is NoOpPlugin", discovered1.iterator().next().getClass(), NoOpPlugin.class);
    Assert.assertEquals("Type is NoOpPlugin", discovered1.iterator().next(), plugin1);

    Collection<DAGExecutionPlugin> discovered2 = locator2.discoverPlugins(conf);
    Assert.assertEquals("Number of plugins discovered ", 1, discovered2.size());
    Assert.assertEquals("Type is NoOpPlugin", discovered2.iterator().next().getClass(), DebugPlugin.class);
    Assert.assertEquals("Type is NoOpPlugin", discovered2.iterator().next(), plugin2);

    ChainedPluginLocator<DAGExecutionPlugin> chained = new ChainedPluginLocator<>(locator1, locator2);
    Collection<DAGExecutionPlugin> chainedDiscovered = chained.discoverPlugins(conf);
    Assert.assertEquals("Number of plugins discovered ", 2, chainedDiscovered.size());
    Assert.assertTrue(chainedDiscovered.contains(plugin1));
    Assert.assertTrue(chainedDiscovered.contains(plugin2));
  }

  @Test
  public void testServicePluginLoader()
  {
    ServiceLoaderBasedPluginLocator<DAGExecutionPlugin> locator = new ServiceLoaderBasedPluginLocator<>(DAGExecutionPlugin.class);
    Collection<DAGExecutionPlugin> discovered = locator.discoverPlugins(conf);
    Assert.assertEquals("Total number of plugins detected ", 1, discovered.size());
    Assert.assertEquals("Type is NoOpPlugin", discovered.iterator().next().getClass(), DebugPlugin.class);
  }

  @Test
  public void testDispatch() throws InterruptedException
  {
    DebugPlugin debugPlugin = new DebugPlugin();
    StaticPluginLocator<? extends DAGExecutionPlugin> locator = new StaticPluginLocator<>(debugPlugin);
    ApexPluginDispatcher pluginManager = new DefaultApexPluginDispatcher(locator,
        new StramTestSupport.TestAppContext(new Attribute.AttributeMap.DefaultAttributeMap()), null, null);
    pluginManager.init(new Configuration());
    int count = debugPlugin.getEventCount();
    pluginManager.dispatch(STRAM_EVENT, new StramEvent(StramEvent.LogLevel.DEBUG)
    {
      @Override
      public String getType()
      {
        return "TestEvent";
      }
    });

    debugPlugin.lock();
    while (debugPlugin.getEventCount() == count) {
      debugPlugin.events.await(5, TimeUnit.SECONDS);
    }
    debugPlugin.unlock();

    Assert.assertEquals("Total stram event received ", debugPlugin.getEventCount(), 1);

    count = debugPlugin.getCommitCount();
    pluginManager.dispatch(COMMIT_EVENT, new Long(1234));
    debugPlugin.lock();
    while (debugPlugin.getCommitCount() == count) {
      debugPlugin.events.await(5, TimeUnit.SECONDS);
    }
    debugPlugin.unlock();

    count = debugPlugin.getHeartbeatCount();
    pluginManager.dispatch(HEARTBEAT, new StreamingContainerUmbilicalProtocol.ContainerHeartbeat());
    debugPlugin.lock();
    while (debugPlugin.getHeartbeatCount() == count) {
      debugPlugin.events.await(5, TimeUnit.SECONDS);
    }
    debugPlugin.unlock();
    pluginManager.stop();

    Assert.assertEquals(1, debugPlugin.getEventCount());
    Assert.assertEquals(1, debugPlugin.getHeartbeatCount());
    Assert.assertEquals(1, debugPlugin.getCommitCount());
  }

}

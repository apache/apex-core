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
package org.apache.apex.engine;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * An implementation of {@link EmbeddedAppLauncher} to launch applications directly in the current Java VM.
 *
 * TODO: When LocalMode is removed, make this class extend EmbeddedAppLauncher directly
 * @since 0.3.2
 */
public class EmbeddedAppLauncherImpl extends LocalMode<EmbeddedAppLauncherImpl.EmbeddedAppHandleImpl>
{
  private final LogicalPlan lp = new LogicalPlan();

  @Override
  public DAG getDAG()
  {
    return lp;
  }

  @Override
  public DAG cloneDAG() throws Exception
  {
    return StramLocalCluster.cloneLogicalPlan(lp);
  }

  @Override
  public EmbeddedAppHandleImpl launchApp(StreamingApplication application, Configuration configuration, Attribute.AttributeMap
      launchParameters) throws LauncherException
  {
    try {
      prepareDAG(application, configuration);
    } catch (Exception e) {
      throw new LauncherException(e);
    }
    StramLocalCluster lc = getController();
    boolean launched = false;
    if (launchParameters != null) {
      if (StramUtils.getValueWithDefault(launchParameters, SERIALIZE_DAG)) {
        // Check if DAG can be serialized
        try {
          cloneDAG();
        } catch (Exception e) {
          throw new LauncherException(e);
        }
      }
      lc.setHeartbeatMonitoringEnabled(StramUtils.getValueWithDefault(launchParameters, HEARTBEAT_MONITORING));
      if (StramUtils.getValueWithDefault(launchParameters, RUN_ASYNC)) {
        lc.runAsync();
        launched = true;
      } else {
        Long runMillis = StramUtils.getValueWithDefault(launchParameters, RUN_MILLIS);
        if (runMillis != null) {
          lc.run(runMillis);
          launched = true;
        }
      }
    }
    if (!launched) {
      lc.run();
    }
    return new EmbeddedAppHandleImpl(lc);
  }

  @Override
  public DAG prepareDAG(StreamingApplication app, Configuration conf) throws Exception
  {
    if (app == null && conf == null) {
      throw new IllegalArgumentException("Require app or configuration to populate logical plan.");
    }
    if (conf == null) {
      conf = new Configuration(false);
    }
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    String appName = app != null ? app.getClass().getName() : "unknown";
    lpc.prepareDAG(lp, app, appName);
    return lp;
  }

  @Override
  public StramLocalCluster getController()
  {
    try {
      addLibraryJarsToClasspath(lp);
      return new StramLocalCluster(lp);
    } catch (Exception e) {
      throw new RuntimeException("Error creating local cluster", e);
    }
  }

  private void addLibraryJarsToClasspath(LogicalPlan lp) throws MalformedURLException
  {
    String libJarsCsv = lp.getAttributes().get(Context.DAGContext.LIBRARY_JARS);

    if (libJarsCsv != null && libJarsCsv.length() != 0) {
      String[] split = libJarsCsv.split(StramClient.LIB_JARS_SEP);
      if (split.length != 0) {
        URL[] urlList = new URL[split.length];
        for (int i = 0; i < split.length; i++) {
          File file = new File(split[i]);
          urlList[i] = file.toURI().toURL();
        }

        // Set class loader.
        ClassLoader prevCl = Thread.currentThread().getContextClassLoader();
        URLClassLoader cl = URLClassLoader.newInstance(urlList, prevCl);
        Thread.currentThread().setContextClassLoader(cl);
      }
    }

  }

  public static class EmbeddedAppHandleImpl implements EmbeddedAppLauncher.EmbeddedAppHandle
  {
    final StramLocalCluster controller;

    public EmbeddedAppHandleImpl(StramLocalCluster controller)
    {
      this.controller = controller;
    }

    @Override
    public boolean isFinished()
    {
      return controller.isFinished();
    }

    @Override
    public void shutdown(ShutdownMode shutdownMode) throws LauncherException
    {
      controller.shutdown();
    }

  }
}

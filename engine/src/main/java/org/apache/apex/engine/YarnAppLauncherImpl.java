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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.apex.api.YarnAppLauncher;
import org.apache.apex.engine.util.StreamingAppFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.base.Throwables;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * An implementation of {@link YarnAppLauncher} to launch applications on Hadoop YARN.
 *
 * @since 3.5.0
 */
public class YarnAppLauncherImpl extends YarnAppLauncher<YarnAppLauncherImpl.YarnAppHandleImpl>
{

  private static final Map<Attribute<?>, String> propMapping = new HashMap<>();

  static {
    propMapping.put(YarnAppLauncher.LIB_JARS, StramAppLauncher.LIBJARS_CONF_KEY_NAME);
    propMapping.put(YarnAppLauncher.ORIGINAL_APP_ID, StramAppLauncher.ORIGINAL_APP_ID);
    propMapping.put(YarnAppLauncher.QUEUE_NAME, StramAppLauncher.QUEUE_NAME);
  }

  @Override
  public YarnAppHandleImpl launchApp(final StreamingApplication app, Configuration conf, Attribute.AttributeMap launchParameters) throws LauncherException
  {
    if (launchParameters != null) {
      for (Map.Entry<Attribute<?>, Object> entry : launchParameters.entrySet()) {
        String property = propMapping.get(entry.getKey());
        if (property != null) {
          setConfiguration(conf, property, entry.getValue());
        }
      }
    }
    try {
      String name = app.getClass().getName();
      StramAppLauncher appLauncher = new StramAppLauncher(name, conf);
      appLauncher.loadDependencies();
      StreamingAppFactory appFactory = new StreamingAppFactory(name, app.getClass())
      {
        @Override
        public LogicalPlan createApp(LogicalPlanConfiguration planConfig)
        {
          return super.createApp(app, planConfig);
        }
      };
      ApplicationId appId = appLauncher.launchApp(appFactory);
      appLauncher.resetContextClassLoader();
      return new YarnAppHandleImpl(appId, conf);
    } catch (Exception ex) {
      throw new LauncherException(ex);
    }
  }

  protected void shutdownApp(YarnAppHandleImpl app, ShutdownMode shutdownMode) throws LauncherException
  {
    if (shutdownMode == ShutdownMode.KILL) {
      try {
        ApplicationId applicationId = app.appId;
        ApplicationReport appReport = app.yarnClient.getApplicationReport(applicationId);
        if (appReport == null) {
          throw new LauncherException("Application " + app.getApplicationId() + " not found");
        }
        app.yarnClient.killApplication(applicationId);
      } catch (YarnException | IOException e) {
        throw Throwables.propagate(e);
      }
    } else {
      throw new UnsupportedOperationException("Orderly shutdown not supported, try kill instead");
    }
  }

  private void setConfiguration(Configuration conf, String property, Object value)
  {
    if (value instanceof Integer) {
      conf.setInt(property, (Integer)value);
    } else if (value instanceof Boolean) {
      conf.setBoolean(property, (Boolean)value);
    } else if (value instanceof Long) {
      conf.setLong(property, (Long)value);
    } else if (value instanceof Float) {
      conf.setFloat(property, (Float)value);
    } else if (value instanceof Double) {
      conf.setDouble(property, (Double)value);
    } else {
      conf.set(property, value.toString());
    }
  }

  public class YarnAppHandleImpl implements YarnAppLauncher.YarnAppHandle, AutoCloseable
  {
    final ApplicationId appId;
    private final YarnClient yarnClient;

    public YarnAppHandleImpl(ApplicationId appId, Configuration conf)
    {
      this.appId = appId;
      this.yarnClient = StramClientUtils.createYarnClient(conf);
    }

    @Override
    public String getApplicationId()
    {
      return appId.toString();
    }

    @Override
    public boolean isFinished()
    {
      try {
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        if (appReport != null) {
          if (appReport.getFinalApplicationStatus() == null
              || appReport.getFinalApplicationStatus() == FinalApplicationStatus.UNDEFINED) {
            return false;
          }
        }
        return true;
      } catch (YarnException | IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void shutdown(org.apache.apex.api.Launcher.ShutdownMode shutdownMode)
        throws org.apache.apex.api.Launcher.LauncherException
    {
      shutdownApp(this, shutdownMode);
    }

    @Override
    public void close() throws Exception
    {
      // Calling close instead of stop on YarnClient as the current close method would typically have been called as
      // part of closeable handling by the vm and it would be appropriate to continue this pattern by calling close on
      // YarnClient. Effectively, this should be the same as calling stop as the documentation of YarnClient close calls
      // for the close method to be a call to stop.
      yarnClient.close();
    }
  }
}

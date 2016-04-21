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
package com.datatorrent.stram;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * <p>LocalModeImpl class.</p>
 *
 * @since 0.3.2
 */
public class LocalModeImpl extends LocalMode
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
  public Controller getController()
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
}

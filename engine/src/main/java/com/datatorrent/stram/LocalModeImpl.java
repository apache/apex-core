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
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

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
public class LocalModeImpl extends LocalMode {

  private final LogicalPlan lp = new LogicalPlan();

  @Override
  public DAG getDAG() {
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

  private void addExtraJarsToClasspath(LogicalPlan lp) throws MalformedURLException
  {
    List<URL> jarUrls = new LinkedList<>();
    for (String jarPath : lp.getJarResources().keySet()) {
      File file = new File(jarPath);
      URL url = file.toURI().toURL();
      jarUrls.add(url);
    }

    ClassLoader prevCl = Thread.currentThread().getContextClassLoader();
    ClassLoader urlCl = URLClassLoader.newInstance(jarUrls.toArray(new URL[jarUrls.size()]), prevCl);
    Thread.currentThread().setContextClassLoader(urlCl);
  }

  @Override
  public Controller getController() {
    try {
      // Add extra jars before the operator are loaded.
      if (lp.getJarResources().size() != 0) {
        addExtraJarsToClasspath(lp);
      }
      return new StramLocalCluster(lp);
    } catch (Exception e) {
      throw new RuntimeException("Error creating local cluster", e);
    }
  }
}

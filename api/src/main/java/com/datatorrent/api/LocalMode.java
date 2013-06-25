/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.api;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;

/**
 * Local mode execution for single application
 */
public abstract class LocalMode {

  abstract public DAG getDAG();
  abstract public DAG cloneDAG() throws Exception;
  abstract public Controller getController();

  public interface Controller {
    public void run();
    public void run(long runMillis);
    public void runAsync();
    public void shutdown();
    public void setHeartbeatMonitoringEnabled(boolean enabled);
  }

  public static LocalMode newInstance() {
    ServiceLoader<LocalMode> loader = ServiceLoader.load(LocalMode.class);
    Iterator<LocalMode> impl = loader.iterator();
    if (!impl.hasNext()) {
      throw new RuntimeException("No implementation for " + LocalMode.class);
    }
    return impl.next();
  }

  /**
   * Shortcut to run an application. Used for testing.
   * @param app
   * @param runMillis
   */
  public static void runApp(StreamingApplication app, int runMillis) {
    LocalMode lma = newInstance();
    app.populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.run(runMillis);
  }

}

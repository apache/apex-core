/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
 * limitations under the License.
 */
package com.datatorrent.api;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;

/**
 * Local mode execution for single application
 *
 * @since 0.3.2
 */
public abstract class LocalMode {

  /**
   * <p>getDAG.</p>
   */
  abstract public DAG getDAG();
  /**
   * <p>cloneDAG.</p>
   */
  abstract public DAG cloneDAG() throws Exception;

  /**
   * Build the logical plan through the given streaming application instance and/or from configuration.
   * <p>
   * The plan will be constructed through {@link StreamingApplication#populateDAG}. If configuration properties are
   * specified, they function as override, as would be the case when launching an application through CLI.
   * <p>
   * This method can also be used to construct the plan declaratively from configuration only, by passing null for the
   * application. In this case the configuration contains all operators and streams.
   * <p>
   *
   * @param app
   * @param conf
   * @return
   * @throws Exception
   * @since 0.3.5
   */
  abstract public DAG prepareDAG(StreamingApplication app, Configuration conf) throws Exception;

  /**
   * <p>getController.</p>
   */
  abstract public Controller getController();

  public interface Controller {
    public void run();
    public void run(long runMillis);
    public void runAsync();
    public void shutdown();
    public void setHeartbeatMonitoringEnabled(boolean enabled);
  }

  /**
   * <p>newInstance.</p>
   */
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
   *
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

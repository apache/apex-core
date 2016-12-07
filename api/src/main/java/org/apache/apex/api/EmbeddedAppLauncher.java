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
package org.apache.apex.api;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

/**
 * Launcher for running the application directly in the current Java VM. For basic operations such as launching or
 * stopping the application, {@link Launcher} can be used directly.
 *
 * @since 3.5.0
 */
public abstract class EmbeddedAppLauncher<H extends EmbeddedAppLauncher.EmbeddedAppHandle> extends Launcher<H>
{
  /**
   * Parameter to specify the time after which the application will be shutdown; pass 0 to run indefinitely.
   */
  public static final Attribute<Long> RUN_MILLIS = new Attribute<>(0L);

  /**
   * Parameter to launch application asynchronously and return from launch immediately.
   */
  public static final Attribute<Boolean> RUN_ASYNC = new Attribute<>(false);

  /**
   * Parameter to enable or disable heartbeat monitoring.
   */
  public static final Attribute<Boolean> HEARTBEAT_MONITORING = new Attribute<>(true);

  /**
   * Parameter to serialize DAG before launch.
   */
  public static final Attribute<Boolean> SERIALIZE_DAG = new Attribute<>(false);

  static {
    Attribute.AttributeMap.AttributeInitializer.initialize(EmbeddedAppLauncher.class);
  }

  public static EmbeddedAppLauncher newInstance()
  {
    return loadService(EmbeddedAppLauncher.class);
  }

  /**
   * The EmbeddedAppHandle class would be useful in future to provide additional information without breaking backwards
   * compatibility of the launchApp method
   */
  public interface EmbeddedAppHandle extends AppHandle {}

  /**
   * <p>
   * getDAG.</p>
   *
   * @return
   */
  public abstract DAG getDAG();

  /**
   * <p>
   * cloneDAG.</p>
   *
   * @return
   * @throws java.lang.Exception
   */
  public abstract DAG cloneDAG() throws Exception;

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
  public abstract DAG prepareDAG(StreamingApplication app, Configuration conf) throws Exception;

  /**
   * Shortcut to run an application. Used for testing.
   *
   * @param app
   * @param runMillis
   */
  public static void runApp(StreamingApplication app, int runMillis)
  {
    runApp(app, null, runMillis);
  }

  /**
   * Shortcut to run an application with the modified configuration.
   *
   * @param app           - Application to be run
   * @param configuration - Configuration
   * @param runMillis     - The time after which the application will be shutdown; pass 0 to run indefinitely.
   */
  public static void runApp(StreamingApplication app, Configuration configuration, int runMillis)
  {
    EmbeddedAppLauncher launcher = newInstance();
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(RUN_MILLIS, (long)runMillis);
    launcher.launchApp(app, configuration, launchAttributes);
  }

}

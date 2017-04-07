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
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.StringCodec;

/**
 * Launcher for running the application on Hadoop YARN. For basic operations such as launching or stopping the
 * application, {@link Launcher} can be used directly.
 *
 * @since 3.5.0
 */
public abstract class YarnAppLauncher<H extends YarnAppLauncher.YarnAppHandle> extends Launcher<H>
{

  /**
   * Parameter to specify extra jars for launch.
   */
  public static final Attribute<String> LIB_JARS = new Attribute<>(new StringCodec.String2String());

  /**
   * Parameter to specify the previous application id to use to resume launch from.
   */
  public static final Attribute<String> ORIGINAL_APP_ID = new Attribute<>(new StringCodec.String2String());

  /**
   * Parameter to specify the queue name to use for launch.
   */
  public static final Attribute<String> QUEUE_NAME = new Attribute<>(new StringCodec.String2String());

  static {
    Attribute.AttributeMap.AttributeInitializer.initialize(YarnAppLauncher.class);
  }

  public static YarnAppLauncher newInstance()
  {
    return loadService(YarnAppLauncher.class);
  }

  public interface YarnAppHandle extends AppHandle
  {
    String getApplicationId();
  }

  /**
   * Shortcut to run an application with the modified configuration.
   *
   * @param app           - Application to be run
   * @param configuration - Application Configuration
   */
  public static void runApp(StreamingApplication app, Configuration configuration) throws LauncherException
  {
    runApp(app, configuration, null);
  }

  /**
   * Shortcut to run an application with the modified configuration.
   *
   * @param app           - Application to be run
   * @param configuration - Application Configuration
   * @param launchAttributes - Launch Configuration
   */
  public static void runApp(StreamingApplication app, Configuration configuration, Attribute.AttributeMap launchAttributes) throws LauncherException

  {
    YarnAppLauncher launcher = newInstance();
    launcher.launchApp(app, configuration, launchAttributes);
  }

}

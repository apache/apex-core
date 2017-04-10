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
package com.datatorrent.api;

import org.apache.apex.api.EmbeddedAppLauncher;

/**
 * Local mode execution for single application
 *
 * @deprecated
 * @since 0.3.2
 */
@Deprecated
public abstract class LocalMode<H extends EmbeddedAppLauncher.EmbeddedAppHandle> extends EmbeddedAppLauncher<H>
{
  /**
   * <p>
   * getController.</p>
   *
   * @return
   */
  public abstract Controller getController();

  public interface Controller
  {
    void run();

    void run(long runMillis);

    void runAsync();

    void shutdown();

    void setHeartbeatMonitoringEnabled(boolean enabled);

  }

  /**
   * <p>
   * newInstance.</p>
   *
   * @return
   */
  public static LocalMode newInstance()
  {
    return loadService(LocalMode.class);
  }

}

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
package org.apache.apex.engine.util;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 *
 *
 * @since 3.5.0
 */
public abstract class StreamingAppFactory implements StramAppLauncher.AppFactory
{
  private Class<?> appClazz;
  private String name;

  public StreamingAppFactory(String name, Class<?> appClazz)
  {
    this.name = name;
    this.appClazz = appClazz;
  }

  public abstract LogicalPlan createApp(LogicalPlanConfiguration planConfig);

  protected LogicalPlan createApp(StreamingApplication app, LogicalPlanConfiguration planConfig)
  {
    return planConfig.createFromStreamingApplication(app, getName());
  }

  public String getName()
  {
    return name;
  }

  public String getDisplayName()
  {
    ApplicationAnnotation an = appClazz.getAnnotation(ApplicationAnnotation.class);
    if (an != null) {
      return an.name();
    } else {
      return name;
    }
  }
}

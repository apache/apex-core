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
package com.datatorrent.common.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>ScheduledThreadPoolExecutor class.</p>
 *
 * @since 0.3.2
 */
public class ScheduledThreadPoolExecutor extends java.util.concurrent.ScheduledThreadPoolExecutor implements ScheduledExecutorService
{
  public static final Logger logger = LoggerFactory.getLogger(ScheduledExecutorService.class);

  public ScheduledThreadPoolExecutor(int corePoolSize, String executorName)
  {
    super(corePoolSize, new NameableThreadFactory(executorName));
  }

  /**
   *
   * @return long
   */
  @Override
  public final long getCurrentTimeMillis()
  {
    return System.currentTimeMillis();
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t)
  {
    super.afterExecute(r, t);
    if (t != null) {
      logger.error("Scheduled task {} died with {}", r, t);
    }
  }
}

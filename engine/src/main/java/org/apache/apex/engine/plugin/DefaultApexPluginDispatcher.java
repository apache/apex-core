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
package org.apache.apex.engine.plugin;

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.api.DAGExecutionPluginContext.Handler;
import org.apache.apex.engine.api.DAGExecutionPluginContext.RegistrationType;
import org.apache.apex.engine.api.PluginLocator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.webapp.AppInfo;

/**
 * Handle dispatching of events from Stram to Plugins. This implementation creates an executor
 * service to process the event asynchronously. A separate task {@link DefaultApexPluginDispatcher.ProcessEventTask}
 * is created to process an event and then submitted to the executor for execution.
 */
public class DefaultApexPluginDispatcher extends ApexPluginManager implements ApexPluginDispatcher
{
  private static final Logger LOG = LoggerFactory.getLogger(DefaultApexPluginDispatcher.class);
  private int qsize = 4098;
  private ArrayBlockingQueue<Runnable> blockingQueue;
  private ExecutorService executorService;

  public DefaultApexPluginDispatcher(PluginLocator locator, StramAppContext context, StreamingContainerManager dmgr, AppInfo.AppStats stats)
  {
    super(locator, context, dmgr, stats);
  }

  @Override
  public <T> void dispatch(RegistrationType<T> registrationType, T data)
  {
    if (executorService != null) {
      executorService.submit(new ProcessEventTask<>(registrationType, data));
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    super.serviceInit(conf);
    LOG.debug("Creating plugin dispatch queue with size {}", qsize);
    blockingQueue = new ArrayBlockingQueue<>(qsize);
    RejectedExecutionHandler rejectionHandler = new RejectedExecutionHandler()
    {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
      {
        try {
          blockingQueue.remove();
          executor.submit(r);
        } catch (NoSuchElementException ex) {
          // Ignore no-such element as queue may finish, while this handler is called.
        }
      }
    };

    executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        blockingQueue, new NameableThreadFactory("PluginExecutorThread"), rejectionHandler);
  }

  @Override
  protected void serviceStop() throws Exception
  {
    executorService.shutdownNow();
    executorService.awaitTermination(10, TimeUnit.SECONDS);
    if (!executorService.isTerminated()) {
      LOG.warn("Executor service still active for plugins");
    }
    executorService = null;
  }

  private class ProcessEventTask<T> implements Runnable
  {
    private final RegistrationType<T> registrationType;
    private final T data;

    public ProcessEventTask(RegistrationType<T> type, T data)
    {
      this.registrationType = type;
      this.data = data;
    }

    @Override
    public void run()
    {
      for (final PluginInfo pInfo : pluginInfoMap.values()) {
        final Handler<T> handler = pInfo.get(registrationType);
        if (handler != null) {
          handler.handle(data);
        }
      }
    }
  }
}

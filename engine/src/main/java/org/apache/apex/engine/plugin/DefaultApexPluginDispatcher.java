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

import org.apache.apex.api.plugin.Event;
import org.apache.apex.api.plugin.Plugin.EventHandler;
import org.apache.apex.engine.api.plugin.DAGExecutionEvent;
import org.apache.apex.engine.api.plugin.PluginLocator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.webapp.AppInfo;

/**
 * Handle dispatching of events from Stram to Plugins. This implementation creates an executor
 * service to process the event asynchronously. A separate task {@link DefaultApexPluginDispatcher.ProcessEventTask}
 * is created to process an event and then submitted to the executor for execution.
 *
 * @since 3.6.0
 */
public class DefaultApexPluginDispatcher extends AbstractApexPluginDispatcher
{
  private static final Logger LOG = LoggerFactory.getLogger(DefaultApexPluginDispatcher.class);
  private static final int TIMEOUT = 10;
  private static final int QUEUE_SIZE = 4098;

  private ArrayBlockingQueue<Runnable> blockingQueue;
  private ExecutorService executorService;

  public DefaultApexPluginDispatcher(PluginLocator locator, StramAppContext context, StreamingContainerManager dmgr, AppInfo.AppStats stats)
  {
    super(DefaultApexPluginDispatcher.class.getName(), locator, context, dmgr, stats);
  }

  @Override
  protected void dispatchExecutionEvent(DAGExecutionEvent event)
  {
    if (executorService != null) {
      executorService.submit(new ProcessEventTask<>(event));
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    super.serviceInit(conf);
    LOG.debug("Creating plugin dispatch queue with size {}", QUEUE_SIZE);
    blockingQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
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
        blockingQueue, new NameableThreadFactory("PluginExecutorThread", true), rejectionHandler);
  }

  @Override
  protected void serviceStop() throws Exception
  {
    if (executorService != null) {
      executorService.shutdown();
      boolean terminated = false;
      try {
        terminated = executorService.awaitTermination(TIMEOUT, TimeUnit.SECONDS);
      } finally {
        if (!terminated) {
          LOG.warn("{} executor service {} failed to terminate withing {} seconds", getClass().getSimpleName(), executorService, TIMEOUT);
        }
        executorService = null;
        super.serviceStop();
      }
    }
  }

  private class ProcessEventTask<T extends DAGExecutionEvent.Type> implements Runnable
  {
    private final Event<T> event;

    public ProcessEventTask(Event<T> event)
    {
      this.event = event;
    }

    @Override
    public void run()
    {
      synchronized (table) {
        for (EventHandler handler : table.row(event.getType()).values()) {
          try {
            handler.handle(event);
          } catch (RuntimeException e) {
            LOG.warn("Event {} caused exception in handler {}", event, handler, e);
          }
        }
      }
    }
  }
}

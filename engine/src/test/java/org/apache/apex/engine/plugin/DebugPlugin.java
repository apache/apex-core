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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.api.plugin.DAGExecutionEvent;
import org.apache.apex.engine.api.plugin.DAGExecutionPlugin;

import com.datatorrent.api.DAG;

import static org.apache.apex.engine.api.plugin.DAGExecutionEvent.Type.COMMIT_EVENT;
import static org.apache.apex.engine.api.plugin.DAGExecutionEvent.Type.HEARTBEAT_EVENT;
import static org.apache.apex.engine.api.plugin.DAGExecutionEvent.Type.STRAM_EVENT;

public class DebugPlugin implements DAGExecutionPlugin<DAGExecutionPlugin.Context>
{
  private static final Logger logger = LoggerFactory.getLogger(DebugPlugin.class);

  private int eventCount = 0;
  private int heartbeatCount = 0;
  private int commitCount = 0;
  CountDownLatch latch = new CountDownLatch(3);
  private Context context;

  @Override
  public void setup(DAGExecutionPlugin.Context context)
  {
    this.context = context;

    context.register(STRAM_EVENT, new EventHandler<DAGExecutionEvent.StramExecutionEvent>()
    {
      @Override
      public void handle(DAGExecutionEvent.StramExecutionEvent event)
      {
        logger.debug("Stram Event {}", event.getStramEvent());
        eventCount++;
        latch.countDown();
      }
    });

    context.register(HEARTBEAT_EVENT, new EventHandler<DAGExecutionEvent.HeartbeatExecutionEvent>()
    {
      @Override
      public void handle(DAGExecutionEvent.HeartbeatExecutionEvent event)
      {
        logger.debug("Heartbeat {}", event.getHeartbeat());
        heartbeatCount++;
        latch.countDown();
      }
    });

    context.register(COMMIT_EVENT, new EventHandler<DAGExecutionEvent.CommitExecutionEvent>()
    {
      @Override
      public void handle(DAGExecutionEvent.CommitExecutionEvent event)
      {
        logger.debug("Commit window id {}", event.getCommitWindow());
        commitCount++;
        latch.countDown();
      }
    });
  }

  @Override
  public void teardown()
  {

  }

  public int getEventCount()
  {
    return eventCount;
  }

  public int getHeartbeatCount()
  {
    return heartbeatCount;
  }

  public int getCommitCount()
  {
    return commitCount;
  }

  public void waitForEventDelivery(long timeout) throws InterruptedException
  {
    latch.await(timeout, TimeUnit.SECONDS);
  }

  public DAG getLogicalPlan()
  {
    return context.getDAG();
  }
}

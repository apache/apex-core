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
package com.datatorrent.stram.engine;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context;
import com.datatorrent.api.StatsListener.OperatorRequest;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.stram.api.BaseContext;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;

/**
 * The for context for all of the operators<p>
 * <br>
 *
 * @since 0.3.2
 */
public class OperatorContext extends BaseContext implements Context.OperatorContext
{
  private Thread thread;
  private long lastProcessedWindowId;
  private final int id;
  private final String name;
  // the size of the circular queue should be configurable. hardcoded to 1024 for now.
  private final CircularBuffer<ContainerStats.OperatorStats> statsBuffer = new CircularBuffer<>(1024);
  private final CircularBuffer<OperatorRequest> requests = new CircularBuffer<>(1024);
  public final boolean stateless;
  private int windowsFromCheckpoint;

  /**
   * The operator to which this context is passed, will timeout after the following milliseconds if no new tuple has been received by it.
   */
  // we should make it configurable somehow.
  private long idleTimeout = 1000L;

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public BlockingQueue<OperatorRequest> getRequests()
  {
    return requests;
  }

  /**
   * @return the idleTimeout
   */
  public long getIdleTimeout()
  {
    return idleTimeout;
  }

  /**
   * @param idleTimeout the idleTimeout to set
   */
  public void setIdleTimeout(long idleTimeout)
  {
    this.idleTimeout = idleTimeout;
  }

  /**
   *
   * @param id the value of id
   * @param name name of the operator
   * @param attributes the value of attributes
   * @param parentContext
   */
  public OperatorContext(int id, @NotNull String name, AttributeMap attributes, Context parentContext)
  {
    super(attributes, parentContext);
    this.lastProcessedWindowId = Stateless.WINDOW_ID;
    this.id = id;
    this.name = Preconditions.checkNotNull(name, "operator name");
    this.stateless = super.getValue(OperatorContext.STATELESS);
  }

  @Override
  public int getId()
  {
    return id;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public int getWindowsFromCheckpoint()
  {
    return windowsFromCheckpoint;
  }

  public void setWindowsFromCheckpoint(int windowsFromCheckpoint)
  {
    this.windowsFromCheckpoint = windowsFromCheckpoint;
  }

  /**
   * Reset counts for next heartbeat interval and return current counts. This is called as part of the heartbeat processing.
   *
   * @param stats
   * @return int
   */
  public final synchronized int drainStats(Collection<? super ContainerStats.OperatorStats> stats)
  {
    //logger.debug("{} draining {}", counters);
    return statsBuffer.drainTo(stats);
  }

  public final synchronized long getLastProcessedWindowId()
  {
    return lastProcessedWindowId;
  }

  public void report(ContainerStats.OperatorStats stats, long windowId)
  {
    lastProcessedWindowId = windowId;
    stats.windowId = windowId;

    stats.counters = this.counters;
    this.counters = null;

    if (!statsBuffer.offer(stats)) {
      statsBuffer.poll();
      statsBuffer.offer(stats);
    }
  }

  public void request(OperatorRequest request)
  {
    //logger.debug("Received request {} for (node={})", request, id);
    requests.add(request);
  }

  public Thread getThread()
  {
    return thread;
  }

  public void setThread(Thread thread)
  {
    this.thread = thread;
  }

  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  private static final long serialVersionUID = 2013060671427L;
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(OperatorContext.class);
}

/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context;

import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.stram.api.BaseContext;
import com.datatorrent.stram.api.NodeRequest;

/**
 * The for context for all of the operators<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class OperatorContext extends BaseContext implements Context.OperatorContext
{
  private static final long serialVersionUID = 2013060671427L;
  private final Thread thread;

  /**
   * @return the thread
   */
  public Thread getThread()
  {
    return thread;
  }

  private long lastProcessedWindowId = -1;
  private final int id;
  // the size of the circular queue should be configurable. hardcoded to 1024 for now.
  private final CircularBuffer<Stats.ContainerStats.OperatorStats> statsBuffer = new CircularBuffer<Stats.ContainerStats.OperatorStats>(1024);
  private final CircularBuffer<NodeRequest> requests = new CircularBuffer<NodeRequest>(1024);
  /**
   * The operator to which this context is passed, will timeout after the following milliseconds if no new tuple has been received by it.
   */
  // we should make it configurable somehow.
  private long idleTimeout = 1000L;

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public BlockingQueue<NodeRequest> getRequests()
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
   * @param worker
   * @param attributes the value of attributes
   * @param parentContext
   */
  public OperatorContext(int id, Thread worker, AttributeMap attributes, Context parentContext)
  {
    super(attributes, parentContext);
    this.id = id;
    this.thread = worker;
  }

  @Override
  public int getId()
  {
    return id;
  }

  /**
   * Reset counts for next heartbeat interval and return current counts. This is called as part of the heartbeat processing.
   *
   * @param stats
   * @return int
   */
  public final synchronized int drainStats(Collection<? super Stats.ContainerStats.OperatorStats> stats)
  {
    //logger.debug("{} draining {}", counters);
    return statsBuffer.drainTo(stats);
  }

  public final synchronized long getLastProcessedWindowId()
  {
    return lastProcessedWindowId;
  }

  synchronized void report(Stats.ContainerStats.OperatorStats stats, long windowId)
  {
    lastProcessedWindowId = windowId;
    stats.windowId = windowId;

    if (!statsBuffer.offer(stats)) {
      statsBuffer.poll();
      statsBuffer.offer(stats);
    }
  }

  public void request(NodeRequest request)
  {
    //logger.debug("Received request {} for (node={})", request, id);
    requests.add(request);
  }

  private static final Logger logger = LoggerFactory.getLogger(OperatorContext.class);
}

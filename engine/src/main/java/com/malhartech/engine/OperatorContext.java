/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.AttributeMap;
import com.malhartech.api.Context;
import com.malhartech.api.Operator;
import com.malhartech.netlet.util.CircularBuffer;

/**
 * The for context for all of the operators<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class OperatorContext implements Context.OperatorContext
{
  private final Thread thread;
  private final Context parentContext;

  /**
   * @return the thread
   */
  public Thread getThread()
  {
    return thread;
  }

  private long lastProcessedWindowId = -1;
  private final int id;
  private final AttributeMap<OperatorContext> attributes;
  // the size of the circular queue should be configurable. hardcoded to 1024 for now.
  private final CircularBuffer<OperatorStats> statsBuffer = new CircularBuffer<OperatorStats>(1024);
  private final CircularBuffer<NodeRequest> requests = new CircularBuffer<NodeRequest>(4);
  /**
   * The operator to which this context is passed, will timeout after the following milliseconds if no new tuple has been received by it.
   */
  // we should make it configurable somehow.
  private long idleTimeout = 1000L;

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
  public OperatorContext(int id, Thread worker, AttributeMap<OperatorContext> attributes, Context parentContext)
  {
    this.id = id;
    this.attributes = attributes;
    this.thread = worker;
    this.parentContext = parentContext;
  }

  @Override
  public int getId()
  {
    return id;
  }

  /**
   * Reset counts for next heartbeat interval and return current counts. This is called as part of the heartbeat processing.
   *
   * @param counters
   * @return int
   */
  public final synchronized int drainHeartbeatCounters(Collection<? super OperatorStats> counters)
  {
    //logger.debug("{} draining {}", counters);
    return statsBuffer.drainTo(counters);
  }

  public final synchronized long getLastProcessedWindowId()
  {
    return lastProcessedWindowId;
  }

  synchronized void report(OperatorStats stats, long windowId)
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

  @Override
  @SuppressWarnings("unchecked")
  public AttributeMap<Context> getAttributes()
  {
    @SuppressWarnings("rawtypes")
    AttributeMap map = attributes;
    return map;
  }

  @Override
  public <T> T attrValue(AttributeMap.AttributeKey<T> key, T defaultValue)
  {
    T retvalue = attributes.attr(key).get();
    if (retvalue == null) {
      if (parentContext == null) {
        return defaultValue;
      }
      else {
        return parentContext.attrValue(key, defaultValue);
      }
    }

    return retvalue;
  }

  public interface NodeRequest
  {
    /**
     * Command to be executed at subsequent end of window.
     *
     * @param operator
     * @param id
     * @param windowId
     * @throws IOException
     */
    public void execute(Operator operator, int id, long windowId) throws IOException;

  }

  private static final Logger logger = LoggerFactory.getLogger(OperatorContext.class);
}

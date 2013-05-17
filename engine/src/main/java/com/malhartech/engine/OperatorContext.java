/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Context;
import com.malhartech.api.DAGContext;
import com.malhartech.api.Operator;
import com.malhartech.api.AttributeMap;
import com.malhartech.util.CircularBuffer;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The for context for all of the operators<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class OperatorContext implements Context.OperatorContext
{
  private final Thread thread;

  /**
   * @return the thread
   */
  public Thread getThread()
  {
    return thread;
  }

  public interface NodeRequest
  {
    /**
     * Command to be executed at subsequent end of window.
     *
     * @param operator
     */
    public void execute(Operator operator, int id, long windowId) throws IOException;

  }

  private long lastProcessedWindowId;
  private final int id;
  private final AttributeMap<OperatorContext> attributes;
  private final AttributeMap<DAGContext> applicationAttributes;
  private final Map<String, AttributeMap<PortContext>> inputPortAttributes;
  private final Map<String, AttributeMap<PortContext>> outputPortAttributes;
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
   * @param applicationAttributes
   * @param inputPortAttributes
   * @param outputPortAttributes
   */
  public OperatorContext(int id, Thread worker, AttributeMap<OperatorContext> attributes, AttributeMap<DAGContext> applicationAttributes,
          Map<String, AttributeMap<PortContext>> inputPortAttributes, Map<String, AttributeMap<PortContext>> outputPortAttributes)
  {
    this.id = id;
    this.attributes = attributes;
    this.applicationAttributes = applicationAttributes;
    this.inputPortAttributes = inputPortAttributes;
    this.outputPortAttributes = outputPortAttributes;
    this.thread = worker;
  }

  @Override
  public int getId()
  {
    return id;
  }

  @Override
  public AttributeMap<DAGContext> getApplicationAttributes()
  {
    return this.applicationAttributes;
  }

  /**
   * Reset counts for next heartbeat interval and return current counts. This is called as part of the heartbeat processing.
   *
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
  public AttributeMap<OperatorContext> getAttributes()
  {
    return this.attributes;
  }

  @Override
  public AttributeMap<PortContext> getInputPortAttributes(String portName)
  {
    return inputPortAttributes == null ? null : inputPortAttributes.get(portName);
  }

  @Override
  public AttributeMap<PortContext> getOutputPortAttributes(String portName)
  {
    return outputPortAttributes == null ? null : outputPortAttributes.get(portName);
  }

  private static final Logger logger = LoggerFactory.getLogger(OperatorContext.class);
}

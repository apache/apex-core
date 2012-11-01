/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.Context;
import com.malhartech.api.Operator;
import com.malhartech.api.Stats;
import com.malhartech.util.AttributeMap;
import com.malhartech.util.CircularBuffer;

/**
 * The for context for all of the operators<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class OperatorContext implements Context.OperatorContext
{
  private static final Logger LOG = LoggerFactory.getLogger(OperatorContext.class);

  public interface NodeRequest
  {
    /**
     * Command to be executed at subsequent end of window.
     * Current used for module state saving, but applicable more widely.
     */
    public void execute(Operator module, String id, long windowId) throws IOException;
  }
  private long lastProcessedWindowId;
  private final String id;
  private final AttributeMap<OperatorContext> attributes;
  // the size of the circular queue should be configurable. hardcoded to 1024 for now.
  private final CircularBuffer<HeartbeatCounters> heartbeatCounters = new CircularBuffer<HeartbeatCounters>(1024);
  private final CircularBuffer<NodeRequest> requests = new CircularBuffer<NodeRequest>(4);
  /**
   * The AbstractModule to which this context is passed, will timeout after the following milliseconds if no new tuple has been received by it.
   */
  // we should make it configurable somehow.
  private long idleTimeout = 1000L;

  public CircularBuffer<NodeRequest> getRequests()
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
   * @param attributes the value of attributes
   */
  public OperatorContext(String id, AttributeMap<OperatorContext> attributes)
  {
    this.id = id;
    this.attributes = attributes;
  }

  @Override
  public String getId()
  {
    return id;
  }

  /**
   * Reset counts for next heartbeat interval and return current counts. This is called as part of the heartbeat processing.
   *
   * @return int
   */
  public final synchronized int drainHeartbeatCounters(Collection<? super HeartbeatCounters> counters)
  {
    return heartbeatCounters.drainTo(counters);
  }

  public final synchronized long getLastProcessedWindowId()
  {
    return lastProcessedWindowId;
  }

  // stats should have tree like structure
  synchronized void report(Map<String, Collection<Stats>> stats, long windowId)
  {
    lastProcessedWindowId = windowId;

    HeartbeatCounters newWindow = new HeartbeatCounters();
    newWindow.windowId = windowId;

    Collection<PortStats> ports = (Collection)stats.get("INPUT_PORTS");
    if (ports != null) {
      for (PortStats s: ports) {
        newWindow.tuplesProcessed += s.processedCount;
      }
    }

    ports = (Collection)stats.get("OUTPUT_PORTS");
    if (ports != null) {
      for (PortStats s: ports) {
        newWindow.tuplesProduced += s.processedCount;
      }
    }

    if (!heartbeatCounters.offer(newWindow)) {
      heartbeatCounters.poll();
      heartbeatCounters.offer(newWindow);
    }
  }

  public void request(NodeRequest request)
  {
    LOG.debug("Received request {} for (node={})", request, id);
    requests.add(request);
  }

  @Override
  public AttributeMap<OperatorContext> getAttributes() {
    return this.attributes;
  }

}

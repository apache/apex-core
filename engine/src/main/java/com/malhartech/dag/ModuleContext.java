/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.util.CircularBuffer;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The for context for all of the operators<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ModuleContext implements Context
{
  private static final Logger LOG = LoggerFactory.getLogger(ModuleContext.class);

  public interface ModuleRequest
  {
    /**
     * Command to be executed at subsequent end of window.
     * Current used for module state saving, but applicable more widely.
     */
    public void execute(Module module, String id, long windowId) throws IOException;
  }
  private long lastProcessedWindowId;
  private final Thread executingThread;
  private final String id;
  // the size of the circular queue should be configurable. hardcoded to 1024 for now.
  private final CircularBuffer<HeartbeatCounters> heartbeatCounters = new CircularBuffer<HeartbeatCounters>(1024);
  private final CircularBuffer<ModuleRequest> requests = new CircularBuffer<ModuleRequest>(4);
  /**
   * The AbstractModule to which this context is passed, will timeout after the following milliseconds if no new tuple has been received by it.
   */
  // we should make it configurable somehow.
  private long idleTimeout = 1000L;

  public CircularBuffer<ModuleRequest> getRequests()
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

  public ModuleContext(String id, Thread t)
  {
    this.id = id;
    executingThread = t;
  }

  public String getId()
  {
    return id;
  }

  /**
   * Reset counts for next heartbeateinterval and return current counts. This is called as part of the heartbeat processing.
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

  synchronized void report(int consumedTupleCount, long processedBytes, long windowId)
  {
    lastProcessedWindowId = windowId;

    HeartbeatCounters newWindow = new HeartbeatCounters();
    newWindow.windowId = windowId;
    newWindow.tuplesProcessed = consumedTupleCount;
    newWindow.bytesProcessed = processedBytes;
    try {
      heartbeatCounters.add(newWindow);
    }
    catch (BufferOverflowException boe) {
      heartbeatCounters.get();
      heartbeatCounters.add(newWindow);
    }
  }

  public void request(ModuleRequest request)
  {
    LOG.debug("Received request {} for (node={})", request, id);
    requests.add(request);
  }

  public Thread getExecutingThread()
  {
    return executingThread;
  }
}

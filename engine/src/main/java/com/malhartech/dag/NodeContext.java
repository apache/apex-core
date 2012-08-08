/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class NodeContext implements Context
{
  public static enum RequestType
  {
    UNDEFINED,
    REPORT,
    BACKUP,
    RESTORE,
    TERMINATE
  }

  public static class HeartbeatCounters
  {
    public volatile long tuplesProcessed;
    public volatile long bytesProcessed;
  }
  private String id;
  private long windowId;
  private volatile HeartbeatCounters heartbeatCounters = new HeartbeatCounters();
  private volatile RequestType request = RequestType.UNDEFINED;
  /**
   * The AbstractNode to which this context is passed, will timeout after the following milliseconds if no new tuple has been received by it.
   */
  // we should make it configurable somehow.
  private static long idleTimeout = 1000L;

  /**
   * @return the requestType
   */
  public final RequestType getRequestType()
  {
    return request;
  }

  /**
   * @param requestType the requestType to set
   */
  public final void setRequestType(RequestType requestType)
  {
    this.request = requestType;
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

  public NodeContext(String id)
  {
    this.id = id;
  }

  public String getId()
  {
    return id;
  }

  public long getCurrentWindowId()
  {
    return windowId;
  }

  public void setCurrentWindowId(long windowId)
  {
    this.windowId = windowId;
  }

  /**
   * Reset counts for next heartbeat interval and return current counts. This is called as part of the heartbeat processing.
   *
   * @return
   */
  public synchronized HeartbeatCounters resetHeartbeatCounters()
  {
    HeartbeatCounters counters = this.heartbeatCounters;
    this.heartbeatCounters = new HeartbeatCounters();
    return counters;
  }

  synchronized void report(int consumedTupleCount)
  {
    // find a way to report the bytes processed.
    this.heartbeatCounters.tuplesProcessed = consumedTupleCount;
    request = RequestType.UNDEFINED;
  }

  void backup(AbstractNode aThis)
  {
    // TBH
    request = RequestType.UNDEFINED;
  }
}

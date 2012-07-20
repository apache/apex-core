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
  public static class HeartbeatCounters {
    public long tuplesProcessed;
    public long bytesProcessed;
  }
  private HeartbeatCounters heartbeatCounters = new HeartbeatCounters();
  
  private String id;
  private long windowId;
  
  /**
   * The AbstractNode to which this context is passed, will timeout after the
   * following milliseconds if no new tuple has been received by it.
   */
  // we should make it configurable somehow.
  private long idleTimeout = 1000;
  
  public NodeContext(String id) {
    this.id = id;
  }

  public String getId() {
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
   * Reset counts for next heartbeat interval and return current counts.
   * This is called as part of the heartbeat processing.
   * @return
   */
  HeartbeatCounters resetHeartbeatCounters() {
     synchronized (this.heartbeatCounters) {
       HeartbeatCounters counters = this.heartbeatCounters;
       this.heartbeatCounters = new HeartbeatCounters();
       return counters;
     }
  }
  
  void countProcessed(Tuple t) {
    synchronized (this.heartbeatCounters) {
      this.heartbeatCounters.tuplesProcessed++;
      // chetan commented this out since counting the serialized size of data is wrong
      //this.heartbeatCounters.bytesProcessed += t.getData().getSerializedSize();
    }
  }
  
}

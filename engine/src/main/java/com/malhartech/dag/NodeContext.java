/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class NodeContext implements Context
{
  public static class HeartbeatCounters {
    public long tuplesProcessed;
    public long bytesProcessed;
  }
  private HeartbeatCounters heartbeatCounters = new HeartbeatCounters();
  
  private String id;
  private long windowId;
  
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

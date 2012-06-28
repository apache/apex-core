package com.malhartech.dag;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Placeholder for stram unit testing
 */
public abstract class DNode implements Configurable {

  public static enum DNodeState {
    NEW, // node instantiated but not processing yet
    PROCESSING,
    IDLE  // the node stopped processing (no more input etc.)
  }

  public static class HeartbeatCounters {
    public long tuplesProcessed;
    public long bytesProcessed;
  }
  
  
  protected Configuration conf;
  private String id;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Reports node state to stram.
   * For testing, this can be used to control topology start/stop.
   * @return
   */
  public abstract DNodeState getState();
  
  /**
   * Transfers to platform and resets counts for next interval.
   * This is called as part of the heartbeat processing.
   * @return
   */
  public HeartbeatCounters getResetCounters() {
     return new HeartbeatCounters();
  }
  
  
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
        append("id", this.id).
        toString();
  }
  
}

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the destination for tuples processed.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamContext implements Context
{
  /**
   * @return the startingWindowId
   */
  public long getStartingWindowId()
  {
    return startingWindowId;
  }

  /**
   * @param startingWindowId the startingWindowId to set
   */
  public void setStartingWindowId(long startingWindowId)
  {
    this.startingWindowId = startingWindowId;
  }

  public String getId()
  {
    return "move the code from BufferStreamContext";
  }

  /**
   * @return the sourceId
   */
  public String getSourceId()
  {
    return sourceId;
  }

  /**
   * @param sourceId the sourceId to set
   */
  public void setSourceId(String upstreamNodeId)
  {
    this.sourceId = upstreamNodeId;
  }

  /**
   * @return the sinkId
   */
  public String getSinkId()
  {
    return sinkId;
  }

  /**
   * @param sinkId the sinkId to set
   */
  public void setSinkId(String downstreamNodeId)
  {
    this.sinkId = downstreamNodeId;
  }

  public static enum State
  {
    UNDEFINED,
    OUTSIDE_WINDOW,
    INSIDE_WINDOW,
    TERMINATED
  }
  private final Logger LOG = LoggerFactory.getLogger(StreamContext.class);
  private Sink sink;
  private SerDe serde;
  private int tupleCount;
  private State sinkState;
  private String sourceId;
  private String sinkId;
  private long startingWindowId;

  public StreamContext(String sourceId, String sinkId)
  {
    this.sourceId = sourceId;
    this.sinkId = sinkId;
    sinkState = State.UNDEFINED;
  }

  public SerDe getSerDe()
  {
    return serde; // required for socket connection
  }

  public void setSerde(SerDe serde)
  {
    this.serde = serde;
  }

  public int getTupleCount()
  {
    return tupleCount;
  }

  State getSinkState()
  {
    return sinkState;
  }

  void setSinkState(State state)
  {
    sinkState = state;
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
    .append("sourceId", sourceId)
    .append("sinkId", sinkId)
    .append("tuples" , tupleCount)
    .append("state", sinkState)
    .toString();
  }
}

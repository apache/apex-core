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
 * Defines the destination for tuples processed<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamContext implements Context
{
  String id;

  public StreamContext(String id)
  {
    this.id = id;
  }

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

  /**
   *
   * @param id
   */
  public void setId(String id)
  {
    this.id = id;
  }

  /**
   *
   * @return String
   */
  public String getId()
  {
    return id;
  }

  /**
   * @return the sourceId
   */
  public String getSourceId()
  {
    return sourceId;
  }

  /**
   * @param upstreamNodeId the sourceId to set
   */
  public void setSourceId(String upstreamNodeId)
  {
    this.sourceId = upstreamNodeId;
  }

  /**
   * @return String (the sink id)
   */
  public String getSinkId()
  {
    return sinkId;
  }

  /**
   * @param downstreamNodeId the sinkId to set this value
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
  private String sourceId;
  private String sinkId;
  private long startingWindowId;

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

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("sourceId", sourceId)
            .append("sinkId", sinkId)
            .append("tuples", tupleCount)
            .toString();
  }
}

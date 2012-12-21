/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.malhartech.api.Context;
import com.malhartech.util.AttributeMap;
import com.malhartech.util.AttributeMap.DefaultAttributeMap;

/**
 * Defines the destination for tuples processed<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamContext extends DefaultAttributeMap<StreamContext> implements Context
{
  private static final long serialVersionUID = 201212042146L;
  public static final AttributeKey<InetSocketAddress> BUFFER_SERVER_ADDRESS = new AttributeKey<InetSocketAddress>("BUFFER_SERVER_ADDRESS");

  public static class AttributeKey<T> extends AttributeMap.AttributeKey<StreamContext, T>
  {
    private AttributeKey(String name)
    {
      super(StreamContext.class, name);
    }
  }

  public InetSocketAddress getBufferServerAddress()
  {
    InetSocketAddress isa = attr(BUFFER_SERVER_ADDRESS).get();
    return new InetSocketAddress(isa.getHostName(), isa.getPort());
  }

  public void setBufferServerAddress(InetSocketAddress isa)
  {
    attr(BUFFER_SERVER_ADDRESS).set(isa);
  }

  public static enum State
  {
    UNDEFINED,
    OUTSIDE_WINDOW,
    INSIDE_WINDOW,
    TERMINATED
  }
  private String sourceId;
  private String sinkId;
  private long startingWindowId;
  private int mask;
  private Set<Integer> partitions;
  private String id;

  /**
   *
   * @param partitionKeys
   */
  public void setPartitions(int mask, Set<Integer> partitionKeys)
  {
    this.mask = mask;
    this.partitions = partitionKeys;
  }

  /**
   *
   */
  public int getPartitionMask()
  {
    return mask;
  }
  /**
   *
   * @return Collection<Integer>
   */
  public Collection<Integer> getPartitions()
  {
    return partitions;
  }

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

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("sourceId", sourceId)
            .append("sinkId", sinkId)
            .toString();
  }
}

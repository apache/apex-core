/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.AttributeMap;
import com.malhartech.api.AttributeMap.DefaultAttributeMap;
import com.malhartech.api.Context;
import com.malhartech.api.StreamCodec;
import com.malhartech.netlet.EventLoop;

/**
 * Defines the destination for tuples processed<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamContext extends DefaultAttributeMap<StreamContext> implements Context
{
  private static final long serialVersionUID = 201212042146L;
  public static final AttributeKey<InetSocketAddress> BUFFER_SERVER_ADDRESS = new AttributeKey<InetSocketAddress>(StreamContext.class, "BUFFER_SERVER");
  public static final AttributeKey<EventLoop> EVENT_LOOP = new AttributeKey<EventLoop>(StreamContext.class, "EVENT_LOOP");
  public static final AttributeKey<StreamCodec<Object>> CODEC = new AttributeKey<StreamCodec<Object>>(StreamContext.class, "CODEC");

  @Override
  @SuppressWarnings("unchecked")
  public AttributeMap<Context> getAttributes()
  {
    @SuppressWarnings("rawtypes")
    AttributeMap context = this;
    return (AttributeMap<Context>)context;
  }

  @Override
  public <T> T attrValue(AttributeKey<T> key, T defaultValue)
  {
    T retvalue = attr(key).get();
    if (retvalue == null) {
      return defaultValue;
    }

    return retvalue;
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
  private long finishedWindowId;
  private int mask;
  private Set<Integer> partitions;
  private String id;

  /**
   *
   * @param mask
   * @param partitionKeys
   */
  public void setPartitions(int mask, Set<Integer> partitionKeys)
  {
    this.mask = mask;
    this.partitions = partitionKeys == null ? null : Collections.unmodifiableSet(partitionKeys);
  }

  /**
   *
   * @return
   */
  public int getPartitionMask()
  {
    return mask;
  }

  /**
   *
   * @return Collection<Integer>
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
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
  public long getFinishedWindowId()
  {
    return finishedWindowId;
  }

  /**
   * @param startingWindowId the startingWindowId to set
   */
  public void setFinishedWindowId(long startingWindowId)
  {
    this.finishedWindowId = startingWindowId;
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

  private static final Logger logger = LoggerFactory.getLogger(StreamContext.class);
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.engine;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context;
import com.datatorrent.api.StreamCodec;

import com.datatorrent.netlet.EventLoop;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;

/**
 * Defines the destination for tuples processed<p>
 * <br>
 *
 * @since 0.3.2
 */
public class StreamContext extends DefaultAttributeMap implements Context
{
  public static final Attribute<InetSocketAddress> BUFFER_SERVER_ADDRESS = new Attribute<>(null, null);
  public static final Attribute<byte[]> BUFFER_SERVER_TOKEN = new Attribute<>(null, null);
  public static final Attribute<EventLoop> EVENT_LOOP = new Attribute<>(null, null);
  public static final Attribute<StreamCodec<?>> CODEC = new Attribute<StreamCodec<?>>(new DefaultStatefulStreamCodec<>(), null);

  @Override
  public AttributeMap getAttributes()
  {
    return this;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    T retvalue = get(key);
    if (retvalue == null) {
      return key.defaultValue;
    }

    return retvalue;
  }

  public InetSocketAddress getBufferServerAddress()
  {
    InetSocketAddress isa = get(BUFFER_SERVER_ADDRESS);
    return new InetSocketAddress(isa.getHostName(), isa.getPort());
  }

  public void setBufferServerAddress(InetSocketAddress isa)
  {
    put(BUFFER_SERVER_ADDRESS, isa);
  }

  @Override
  public void setCounters(Object counters)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void sendMetrics(Collection<String> metricNames)
  {
    throw new UnsupportedOperationException("Not supported yet.");
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
  private String portId;

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

  public String getPortId()
  {
    return portId;
  }

  public void setPortId(String portId)
  {
    this.portId = portId;
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("sourceId", sourceId)
            .append("sinkId", sinkId)
            .toString();
  }

  static {
    AttributeInitializer.initialize(StreamContext.class);
  }

  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  private static final long serialVersionUID = 201212042146L;
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(StreamContext.class);
}

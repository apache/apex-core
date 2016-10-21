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
package com.datatorrent.stram.api;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamCodec;

/**
 * Operator deployment info passed from master to container as part of initialization
 * or incremental undeploy/deploy during recovery, balancing or other modification.
 *
 * @since 0.3.2
 */
public class OperatorDeployInfo implements Serializable, OperatorContext
{
  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  private static final long serialVersionUID = 201208271956L;

  @Override
  public int getId()
  {
    return id;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public AttributeMap getAttributes()
  {
    return contextAttributes;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    T get = contextAttributes.get(key);
    if (get == null) {
      return key.defaultValue;
    }

    return get;
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

  @Override
  public int getWindowsFromCheckpoint()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public enum OperatorType
  {
    INPUT, UNIFIER, GENERIC, OIO
  }

  public static class UnifierDeployInfo extends OperatorDeployInfo
  {
    public AttributeMap operatorAttributes;

    public UnifierDeployInfo()
    {
      type = OperatorType.UNIFIER;
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201412091818L;
  }

  /**
   * Input to node, either inline or from socket stream.
   */
  public static class InputDeployInfo implements Serializable, PortContext
  {
    public Locality locality;
    /**
     * Port name matching the operator's port declaration
     */
    public String portName;
    /**
     * Name of stream declared in logical topology
     */
    public String declaredStreamId;
    /**
     * If inline connection, id of source node in same container.
     * For buffer server, upstream publisher node id.
     */
    public int sourceNodeId;
    /**
     * Port of the upstream node from where this input stream originates.
     * Required to uniquely identify publisher end point.
     */
    public String sourcePortName;
    /**
     * Buffer server subscriber info, set only when upstream operator not in same container.
     */
    public String bufferServerHost;
    public int bufferServerPort;
    public byte[] bufferServerToken;
    /**
     * Class name of tuple SerDe (buffer server stream only).
     */
    /*
     @Deprecated
     public String serDeClassName;
     */
    /**
     * The SerDe object.
     */
    /*
     public StreamCodec streamCodec;
     */
    public Map<Integer, StreamCodec<?>> streamCodecs = new HashMap<>();
    /**
     * Partition keys for the input stream. Null w/o partitioning.
     */
    public Set<Integer> partitionKeys;
    public int partitionMask;
    /**
     * Context attributes for input port
     */
    public AttributeMap contextAttributes;

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("portName", this.portName)
          .append("streamId", this.declaredStreamId)
          .append("sourceNodeId", this.sourceNodeId)
          .append("sourcePortName", this.sourcePortName)
          .append("locality", this.locality)
          .append("partitionMask", this.partitionMask)
          .append("partitionKeys", this.partitionKeys)
          .toString();
    }

    @Override
    public AttributeMap getAttributes()
    {
      return contextAttributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T get = contextAttributes.get(key);
      if (get == null) {
        return key.defaultValue;
      }

      return get;
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

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201208271957L;
  }

  /**
   * Node output, publisher info.
   * Streams can have multiple sinks, hence output won't reference target node or port.
   * For inline streams, input info will have source node for wiring.
   * For buffer server output, node id/port will be used as publisher id and referenced by subscribers.
   */
  public static class OutputDeployInfo implements PortContext, Serializable
  {
    /**
     * Port name matching the node's port declaration
     */
    public String portName;
    /**
     * Name of stream declared in logical topology
     */
    public String declaredStreamId;
    /**
     * Buffer server publisher info, set when stream not inline.
     */
    public String bufferServerHost;
    public int bufferServerPort;
    public byte[] bufferServerToken;
    public Map<Integer, StreamCodec<?>> streamCodecs = new HashMap<>();
    /**
     * Context attributes for output port
     */
    public AttributeMap contextAttributes;

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
              .append("portName", this.portName)
              .append("streamId", this.declaredStreamId)
              .append("bufferServer", this.bufferServerHost)
              .toString();
    }

    @Override
    public AttributeMap getAttributes()
    {
      return contextAttributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T attr = contextAttributes.get(key);
      if (attr == null) {
        return key.defaultValue;
      }

      return attr;
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

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201208271958L;
  }

  /**
   * Unique id in the DAG, assigned by the master and immutable (restart/recovery)
   */
  public int id;
  /**
   * Type of the operator. Required as single class
   * may implement regular operator and unifier.
   */
  public OperatorType type;
  /**
   * Logical operator name from DAG.
   */
  public String name;
  /**
   * The checkpoint window identifier.
   * Used to restore state and incoming streams as part of recovery.
   */
  public Checkpoint checkpoint;
  /**
   * Inputs to node, either from socket stream or inline from other node(s).
   */
  public List<InputDeployInfo> inputs;
  /**
   * Outputs from node, either to socket stream or inline to other node(s).
   */
  public List<OutputDeployInfo> outputs;
  /**
   * Context attributes for operator
   */
  public AttributeMap contextAttributes;

  /**
   *
   * @return String
   */
  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", this.id)
        .append("name", this.name)
        .append("type", this.type)
        .append("checkpoint", this.checkpoint)
        .append("inputs", this.inputs)
        .append("outputs", this.outputs)
        .toString();
  }

}

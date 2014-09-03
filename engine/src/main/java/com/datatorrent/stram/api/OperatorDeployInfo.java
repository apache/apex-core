/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.api;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamCodec;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Operator deployment info passed from master to container as part of initialization
 * or incremental undeploy/deploy during recovery, balancing or other modification.
 *
 * @since 0.3.2
 */
public class OperatorDeployInfo implements Serializable
{
  private static final long serialVersionUID = 201208271956L;

  public enum OperatorType
  {
    INPUT, UNIFIER, GENERIC, OIO
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
    public Map<PortIdentifier, StreamCodecInfo> streamCodecs = new HashMap<PortIdentifier, StreamCodecInfo>();
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
    public <T> T getValue(AttributeMap.Attribute<T> key)
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
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
    public Map<PortIdentifier, StreamCodecInfo> streamCodecs = new HashMap<PortIdentifier, StreamCodecInfo>();
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
    public <T> T getValue(AttributeMap.Attribute<T> key)
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
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201208271958L;
  }

  public static class StreamCodecInfo implements Serializable
  {

    /**
     * Class name of tuple SerDe (buffer server stream only).
     */
    @Deprecated
    public String serDeClassName;

    /**
     * The SerDe object.
     */
    public StreamCodec<?> streamCodec;
  }

  public static class PortIdentifier implements Serializable
  {
    /*
     * The operator name
     */
    public String operName;

    /**
     * The port name
     */
    public String portName;

    @Override
    public boolean equals(Object o)
    {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PortIdentifier portIdentifier = (PortIdentifier) o;

      if (!operName.equals(portIdentifier.operName)) return false;
      if (!portName.equals(portIdentifier.portName)) return false;

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = operName.hashCode();
      result = 31 * result + portName.hashCode();
      return result;
    }
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
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", this.id).
            append("name", this.name).
            append("type", this.type).
            append("checkpoint", this.checkpoint).
            append("inputs", this.inputs).
            append("outputs", this.outputs).
            toString();
  }

}

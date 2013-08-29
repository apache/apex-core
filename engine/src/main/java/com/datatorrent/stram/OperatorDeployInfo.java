/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context.PortContext;

/**
 * Operator deployment info passed from master to container as part of initialization
 * or incremental undeploy/deploy during recovery, balancing or other modification.
 *
 * @since 0.3.2
 */
public class OperatorDeployInfo implements Serializable
{
  private static final long serialVersionUID = 201208271956L;

  public enum OperatorType {
    INPUT, UNIFIER, GENERIC
  }

  /**
   * Input to node, either inline or from socket stream.
   */
  public static class InputDeployInfo implements Serializable, PortContext
  {
    private static final long serialVersionUID = 201208271957L;

    public boolean isInline() {
      return bufferServerHost == null;
    }

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
     * Buffer server subscriber info, set only when stream is not inline.
     */
    public String bufferServerHost;

    public int bufferServerPort;

    /**
     * Class name of tuple SerDe (buffer server stream only).
     */
    public String serDeClassName;

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
                .append("inline", this.isInline())
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
    public <T> T attrValue(AttributeMap.AttributeKey<T> key, T defaultValue)
    {
      T retvalue = contextAttributes.attr(key).get();
      if (retvalue == null) {
        return defaultValue;
      }

      return retvalue;
    }


  }

  /**
   * Node output, publisher info.
   * Streams can have multiple sinks, hence output won't reference target node or port.
   * For inline streams, input info will have source node for wiring.
   * For buffer server output, node id/port will be used as publisher id and referenced by subscribers.
   */
  public static class OutputDeployInfo implements PortContext, Serializable
  {
    private static final long serialVersionUID = 201208271958L;

    public boolean isInline() {
      return bufferServerHost == null;
    }

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
    public String serDeClassName;

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
                .append("inline", this.isInline())
                .toString();
    }

    @Override
    public AttributeMap getAttributes()
    {
      return contextAttributes;
    }

    @Override
    public <T> T attrValue(AttributeMap.AttributeKey<T> key, T defaultValue)
    {
      T retvalue = contextAttributes.attr(key).get();
      if (retvalue == null) {
        return defaultValue;
      }

      return retvalue;
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
  public String declaredId;

  /**
   * The checkpoint window identifier.
   * Used to restore state and incoming streams as part of recovery.
   * Value 0 indicates fresh initialization, no restart.
   */
  public long checkpointWindowId = -1;

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
            append("declaredId", this.declaredId).
            append("checkpoint", this.checkpointWindowId).
            append("inputs", this.inputs).
            append("outputs", this.outputs).
            toString();
  }

}

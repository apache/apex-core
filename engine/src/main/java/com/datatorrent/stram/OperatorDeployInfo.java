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
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.stram.engine.WindowGenerator;

/**
 * Operator deployment info passed from master to container as part of initialization
 * or incremental undeploy/deploy during recovery, balancing or other modification.
 *
 * @since 0.3.2
 */
public class OperatorDeployInfo implements Serializable
{
  private static final long serialVersionUID = 201208271956L;
  /**
   * WindowId used to store the state of the operator which has not processed a single tuple.
   */
  public static final long STATELESS_CHECKPOINT_WINDOW_ID = WindowGenerator.MIN_WINDOW_ID - 1;

  public enum OperatorType {
    INPUT, UNIFIER, GENERIC, OIO
  }

  /**
   * Input to node, either inline or from socket stream.
   */
  public static class InputDeployInfo implements Serializable, PortContext
  {
    private static final long serialVersionUID = 201208271957L;

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
    public <T> T attrValue(AttributeMap.AttributeKey<T> key, T defaultValue)
    {
      AttributeMap.Attribute<T> attr = contextAttributes.attrOrNull(key);
      if (attr == null || attr.get() == null) {
        return defaultValue;
      }

      return attr.get();
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

    /**
     * Runtime attributes for output port.
     * Will be merged with contextAttributes when new attribute API is available.
     */
    public AttributeMap runtimeAttributes;

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
    public <T> T attrValue(AttributeMap.AttributeKey<T> key, T defaultValue)
    {
      AttributeMap.Attribute<T> attr = contextAttributes.attrOrNull(key);
      if (attr == null || attr.get() == null) {
        return defaultValue;
      }

      return attr.get();
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
   */
  public long checkpointWindowId = STATELESS_CHECKPOINT_WINDOW_ID;

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

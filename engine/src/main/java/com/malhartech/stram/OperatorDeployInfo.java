/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.Serializable;
import java.util.List;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.malhartech.api.Context;

/**
 * Operator deployment info passed from master to container as part of initialization
 * or incremental undeploy/deploy during recovery, balancing or other modification.
 */
public class OperatorDeployInfo implements Serializable
{
  private static final long serialVersionUID = 201208271956L;

  /**
   * Input to node, either inline or from socket stream.
   */
  public static class InputDeployInfo implements Serializable
  {
    private static final long serialVersionUID = 201208271957L;

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
     * If inline connection, id of source node in same container.
     * For buffer server, upstream publisher node id.
     */
    public String sourceNodeId;

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
     * Partition keys. For dynamic partitioning, set is initially empty (after
     * topology initialization) and will be populated from node processing stats
     * if the node emits partitioned data. Value(s), once assigned assigned by
     * stram limit what data flows between 2 physical operators. Once values are set,
     * node uses them subscribe to buffer server. Stram may request detailed
     * partition stats as heartbeat response, based on which it can load balance
     * (split/merge operators) if node is elastic.
     */
    public List<byte[]> partitionKeys;

    /**
     * Context attributes for input port
     */
    public Context.SerializableAttributeMap contextAttributes;

    @Override
    public String toString()
    {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("portName", this.portName)
                .append("streamId", this.declaredStreamId)
                .append("inline", this.isInline())
                .toString();
    }

  }

  /**
   * Node output, publisher info.
   * Streams can have multiple sinks, hence output won't reference target node or port.
   * For inline streams, input info will have source node for wiring.
   * For buffer server output, node id/port will be used as publisher id and referenced by subscribers.
   */
  public static class OutputDeployInfo implements Serializable
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
    public Context.SerializableAttributeMap contextAttributes;

    @Override
    public String toString()
    {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("portName", this.portName)
                .append("streamId", this.declaredStreamId)
                .append("inline", this.isInline())
                .toString();
    }

  }

  /**
   * Serialized state of the node. Either by serializing the declared node object or checkpoint state.
   */
  public byte[] serializedNode;

  /**
   * Unique id in the DAG, assigned by the master and immutable (restart/recovery)
   */
  public String id;

  /**
   * Logical node name from the topology declaration.
   */
  public String declaredId;

  /**
   * The checkpoint window identifier.
   * Used to restore node and incoming streams as part of recovery.
   * Value 0 indicates fresh initialization, no restart.
   */
  public long checkpointWindowId = 0;

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
  public Context.SerializableAttributeMap contextAttributes;


  /**
   *
   * @return String
   */
  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", this.id).
            append("declaredId", this.declaredId).
            append("inputs", this.inputs).
            append("outputs", this.outputs).
            toString();
  }

}

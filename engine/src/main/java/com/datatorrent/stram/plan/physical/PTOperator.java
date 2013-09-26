/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.datatorrent.api.Operator;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.PartitionableOperator.Partition;
import com.datatorrent.api.PartitionableOperator.PartitionKeys;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.physical.PhysicalPlan.StatsHandler;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 *
 * Representation of an operator in the physical layout<p>
 * <br>
 *
 */
public class PTOperator {

  public enum State {
    NEW,
    PENDING_DEPLOY,
    ACTIVE,
    PENDING_UNDEPLOY,
    INACTIVE,
    REMOVED
  }

  /**
   *
   * Representation of an input in the physical layout. A source in the DAG
   * <p>
   * <br>
   */
  public static class PTInput
  {
    public final LogicalPlan.StreamMeta logicalStream;
    public final PTOperator target;
    public final PartitionKeys partitions;
    public final PTOutput source;
    public final String portName;

    /**
     *
     * @param portName
     * @param logicalStream
     * @param target
     * @param partitions
     * @param source
     */
    protected PTInput(String portName, StreamMeta logicalStream, PTOperator target, PartitionKeys partitions, PTOutput source)
    {
      this.logicalStream = logicalStream;
      this.target = target;
      this.partitions = partitions;
      this.source = source;
      this.portName = portName;
      this.source.sinks.add(this);
    }

    /**
     *
     * @return String
     */
    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("target", this.target).append("port", this.portName).append("stream", this.logicalStream.getId()).toString();
    }

  }

  /**
   *
   * Representation of an output in the physical layout. A sink in the DAG
   * <p>
   * <br>
   */
  public static class PTOutput
  {
    public final LogicalPlan.StreamMeta logicalStream;
    public final PTOperator source;
    public final String portName;
    final PhysicalPlan plan;
    public final List<PTInput> sinks;

    /**
     * Constructor
     *
     * @param plan
     * @param portName
     * @param logicalStream
     * @param source
     */
    protected PTOutput(PhysicalPlan plan, String portName, StreamMeta logicalStream, PTOperator source)
    {
      this.plan = plan;
      this.logicalStream = logicalStream;
      this.source = source;
      this.portName = portName;
      this.sinks = new ArrayList<PTInput>();
    }

    /**
     * Determine whether downstream operators are deployed inline. (all
     * instances of the downstream operator are in the same container)
     *
     * @return boolean
     */
    public boolean isDownStreamInline()
    {
      for (PTInput sink : this.sinks) {
        if (this.source.container != sink.target.container) {
          return false;
        }
      }
      return true;
    }

   /**
    *
    * @return String
    */
   @Override
   public String toString() {
     return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
         append("source", this.source).
         append("port", this.portName).
         append("stream", this.logicalStream.getId()).
         toString();
   }

 }

  PTOperator(PhysicalPlan plan, int id, String name) {
    this.plan = plan;
    this.name = name;
    this.id = id;
  }

  private PTOperator.State state = State.NEW;
  private final PhysicalPlan plan;
  PTContainer container;
  LogicalPlan.OperatorMeta logicalNode;
  final int id;
  private final String name;
  Partition<?> partition;
  Operator unifier;
  List<PTInput> inputs;
  List<PTOutput> outputs;
  public final LinkedList<Long> checkpointWindows = new LinkedList<Long>();
  long recoveryCheckpoint = 0;
  public int failureCount = 0;
  int loadIndicator = 0;
  public List<? extends StatsHandler> statsMonitors;

  final Map<Locality, Set<PTOperator>> groupings = Maps.newHashMapWithExpectedSize(3);

  public List<StreamingContainerUmbilicalProtocol.StramToNodeRequest> deployRequests = Collections.emptyList();

  final HashMap<InputPortMeta, PTOperator> upstreamMerge = new HashMap<InputPortMeta, PTOperator>();

  /**
   *
   * @return Operator
   */
  public OperatorMeta getOperatorMeta() {
    return this.logicalNode;
  }

  public PTOperator.State getState() {
    return state;
  }

  public void setState(PTOperator.State state) {
    this.state = state;
  }

  /**
   * Return the most recent checkpoint for this operator,
   * representing the last getSaveStream reported.
   * @return long
   */
  public long getRecentCheckpoint() {
    if (checkpointWindows != null && !checkpointWindows.isEmpty()) {
      return checkpointWindows.getLast();
    }
    return 0;
  }

  /**
   * Return the checkpoint that can be used for recovery. This may not be the
   * most recent checkpoint, depending on downstream state.
   *
   * @return long
   */
  public long getRecoveryCheckpoint()
  {
    return recoveryCheckpoint;
  }

  public void setRecoveryCheckpoint(long recoveryCheckpoint)
  {
    this.recoveryCheckpoint = recoveryCheckpoint;
  }

  /**
   *
   * @return String
   */
  public String getLogicalId() {
    return logicalNode.getName();
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public PhysicalPlan getPlan() {
    return plan;
  }

  public List<PTInput> getInputs()
  {
    return inputs;
  }

  public List<PTOutput> getOutputs()
  {
    return outputs;
  }

  public PTContainer getContainer() {
    return container;
  }

  public Partition<?> getPartition() {
    return partition;
  }

  public Operator getUnifier()
  {
    return unifier;
  }

  Set<PTOperator> getGrouping(Locality type) {
    Set<PTOperator> s = this.groupings.get(type);
    if (s == null) {
      s = Sets.newHashSet();
      this.groupings.put(type, s);
    }
    return s;
  }

  public Set<PTOperator> getNodeLocalOperators() {
    return getGrouping(Locality.NODE_LOCAL);
  }

  /**
   *
   * @return String
   */
  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", id).append("name", name).toString();
  }

}
/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.physical;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.stram.Journal.SetOperatorState;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.*;

/**
 *
 * Representation of an operator in the physical layout<p>
 * <br>
 *
 * @since 0.3.5
 */
public class PTOperator implements java.io.Serializable
{
  private static final long serialVersionUID = 201312112033L;

  public enum State {
    PENDING_DEPLOY,
    ACTIVE,
    PENDING_UNDEPLOY,
    INACTIVE
  }

  /**
   *
   * Representation of an input in the physical layout. A source in the DAG
   * <p>
   * <br>
   */
  public static class PTInput implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312112033L;

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
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("o", this.target).append("port", this.portName).append("source", this.source).toString();
    }

  }

  /**
   *
   * Representation of an output in the physical layout. A sink in the DAG
   * <p>
   * <br>
   */
  public static class PTOutput implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312112033L;

    public final LogicalPlan.StreamMeta logicalStream;
    public final PTOperator source;
    public final String portName;
    public final List<PTInput> sinks;

    /**
     * Constructor
     *
     * @param portName
     * @param logicalStream
     * @param source
     */
    protected PTOutput(String portName, StreamMeta logicalStream, PTOperator source)
    {
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
         append("o", this.source).
         append("port", this.portName).
         append("stream", this.logicalStream.getName()).
         toString();
   }

 }

  PTOperator(PhysicalPlan plan, int id, String name, OperatorMeta om)
  {
    this.checkpoints = new LinkedList<Checkpoint>();
    this.plan = plan;
    this.name = name;
    this.id = id;
    this.operatorMeta = om;
    this.stats = new OperatorStatus(this.id, om);
  }

  private volatile PTOperator.State state = State.INACTIVE;
  private final PhysicalPlan plan;
  PTContainer container;
  final LogicalPlan.OperatorMeta operatorMeta;
  final int id;
  private final String name;
  Map<InputPortMeta, PartitionKeys> partitionKeys;
  Class<?> unifierClass;
  List<PTInput> inputs;
  List<PTOutput> outputs;
  public final LinkedList<Checkpoint> checkpoints;
  Checkpoint recoveryCheckpoint;
  public int failureCount = 0;
  public int loadIndicator = 0;
  public List<? extends StatsListener> statsListeners;
  public final OperatorStatus stats;

  final Map<Locality, HostOperatorSet> groupings = Maps.newHashMapWithExpectedSize(3);

  public List<StreamingContainerUmbilicalProtocol.StramToNodeRequest> deployRequests = Collections.emptyList();

  public final HashMap<InputPortMeta, PTOperator> upstreamMerge = new HashMap<InputPortMeta, PTOperator>();

  //check if there is need for this
  public transient Object lastSeenCounters;

  /**
   *
   * @return Operator
   */
  public OperatorMeta getOperatorMeta() {
    return this.operatorMeta;
  }

  public PTOperator.State getState() {
    return state;
  }

  public void setState(PTOperator.State state) {
    this.getPlan().getContext().writeJournal(SetOperatorState.newInstance(this.getId(), state));
    this.state = state;
  }

  /**
   * Return the most recent checkpoint for this operator,
   * representing the last getSaveStream reported.
   * @return long
   */
  public Checkpoint getRecentCheckpoint() {
    if (checkpoints.isEmpty()) {
      return Checkpoint.INITIAL_CHECKPOINT;
    }

    return checkpoints.getLast();
  }

  /**
   * Return the checkpoint that can be used for recovery. This may not be the
   * most recent checkpoint, depending on downstream state.
   *
   * @return long
   */
  public Checkpoint getRecoveryCheckpoint()
  {
    return recoveryCheckpoint;
  }

  public void setRecoveryCheckpoint(Checkpoint recoveryCheckpoint)
  {
    this.recoveryCheckpoint = recoveryCheckpoint;
  }

  /**
   *
   * @return String
   */
  public String getLogicalId() {
    return operatorMeta.getName();
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

  public Map<InputPort<?>, PartitionKeys> getPartitionKeys() {
    Map<InputPort<?>,PartitionKeys> pkeys = null;
    if (partitionKeys != null) {
      pkeys = Maps.newHashMapWithExpectedSize(partitionKeys.size());
      for (Map.Entry<InputPortMeta, PartitionKeys> e : partitionKeys.entrySet()) {
        pkeys.put(e.getKey().getPortObject(), e.getValue());
      }
    }
    return pkeys;
  }

  public void setPartitionKeys(Map<InputPort<?>, PartitionKeys> keys) {
    this.partitionKeys = OperatorPartitions.convertPartitionKeys(this, keys);
  }

  public boolean isUnifier()
  {
    return unifierClass != null;
  }

  public Class<?> getUnifierClass()
  {
    return unifierClass;
  }

  public boolean isOperatorStateLess()
  {
    if (operatorMeta.getDAG().getValue(OperatorContext.STATELESS) || operatorMeta.getValue(OperatorContext.STATELESS)) {
      return true;
    }

    if (unifierClass != null) {
      return unifierClass.isAnnotationPresent(Stateless.class);
    }

    return operatorMeta.getOperator().getClass().isAnnotationPresent(Stateless.class);
  }

  public Checkpoint addCheckpoint(long windowId, long startTime)
  {
    int widthMillis = operatorMeta.getDAG().getValue(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS);
    long millis = WindowGenerator.getWindowMillis(windowId, startTime, widthMillis);
    long count = WindowGenerator.getWindowCount(millis, startTime, widthMillis);
    Checkpoint c = new Checkpoint(windowId, (int)(count % operatorMeta.getValue(OperatorContext.APPLICATION_WINDOW_COUNT)), (int)(count % operatorMeta.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT)));
    this.checkpoints.add(c);
    return c;
  }

  HostOperatorSet getGrouping(Locality type) {
    HostOperatorSet grpObj = this.groupings.get(type);
    if (grpObj == null) {
      grpObj = new HostOperatorSet();
      grpObj.operatorSet = Sets.newHashSet();
      this.groupings.put(type, grpObj);
    }
    return grpObj;
  }

  public HostOperatorSet getNodeLocalOperators() {
    return getGrouping(Locality.NODE_LOCAL);
  }

  public class HostOperatorSet implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312112033L;

    private String host;
    private Set<PTOperator> operatorSet;
    public String getHost()
    {
      return host;
    }
    public void setHost(String host)
    {
      this.host = host;
    }
    public Set<PTOperator> getOperatorSet()
    {
      return operatorSet;
    }
    public void setOperatorSet(Set<PTOperator> operatorSet)
    {
      this.operatorSet = operatorSet;
    }

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

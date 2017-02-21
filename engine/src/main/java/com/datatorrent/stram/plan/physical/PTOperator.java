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
package com.datatorrent.stram.plan.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.stram.Journal.Recoverable;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;

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

  public static final Recoverable SET_OPERATOR_STATE = new SetOperatorState();

  public enum State
  {
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
    public final boolean delay;

    /**
     *
     * @param portName
     * @param logicalStream
     * @param target
     * @param partitions
     * @param source
     */
    protected PTInput(String portName, StreamMeta logicalStream, PTOperator target, PartitionKeys partitions, PTOutput source, boolean delay)
    {
      this.logicalStream = logicalStream;
      this.target = target;
      this.partitions = partitions;
      this.source = source;
      this.portName = portName;
      this.source.sinks.add(this);
      this.delay = delay;
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
      this.sinks = new ArrayList<>();
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

    public Set<PTOperator> threadLocalSinks()
    {
      Set<PTOperator> threadLocalOperators = null;
      if (logicalStream != null && logicalStream.getLocality() == Locality.THREAD_LOCAL) {
        threadLocalOperators = new HashSet<>();
        for (PTInput sink : this.sinks) {
          threadLocalOperators.add(sink.target);
        }
      }
      return threadLocalOperators;
    }

    /**
     * @return String
     */
    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("o", this.source)
          .append("port", this.portName)
          .append("stream", this.logicalStream.getName())
          .toString();
    }

  }

  private static class SetOperatorState implements Recoverable
  {
    private final int operatorId;
    private final PTOperator.State state;

    private SetOperatorState()
    {
      operatorId = -1;
      state = PTOperator.State.INACTIVE;
    }

    private SetOperatorState(int operatorId, PTOperator.State state)
    {
      this.operatorId = operatorId;
      this.state = state;
    }

    @Override
    public void read(final Object object, final Input in) throws KryoException
    {
      PhysicalPlan plan = (PhysicalPlan)object;

      int operatorId = in.readInt();
      int stateOrd = in.readInt();
      plan.getAllOperators().get(operatorId).state = PTOperator.State.values()[stateOrd];
    }

    @Override
    public void write(final Output out) throws KryoException
    {
      out.writeInt(operatorId);
      out.writeInt(state.ordinal());
    }

  }

  PTOperator(PhysicalPlan plan, int id, String name, OperatorMeta om)
  {
    this.checkpoints = new LinkedList<>();
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
  OperatorMeta unifiedOperatorMeta; /* this is the meta for the operator for which this unifier exists */
  List<PTInput> inputs;
  List<PTOutput> outputs;
  public final LinkedList<Checkpoint> checkpoints;
  Checkpoint recoveryCheckpoint;
  public int failureCount = 0;
  public int loadIndicator = 0;
  public List<? extends StatsListener.StatsListenerWithContext> statsListeners;
  public final OperatorStatus stats;

  final Map<Locality, HostOperatorSet> groupings = Maps.newHashMapWithExpectedSize(3);

  public List<StreamingContainerUmbilicalProtocol.StramToNodeRequest> deployRequests = Collections.emptyList();

  public final HashMap<InputPortMeta, PTOperator> upstreamMerge = new HashMap<>();

  /**
   * @return Operator
   */
  public OperatorMeta getOperatorMeta()
  {
    return this.operatorMeta;
  }

  public PTOperator.State getState()
  {
    return state;
  }

  public void setState(PTOperator.State state)
  {
    this.getPlan().getContext().writeJournal(new SetOperatorState(getId(), state));
    this.state = state;
  }

  /**
   * Return the most recent checkpoint for this operator,
   * representing the last getSaveStream reported.
   *
   * @return long
   */
  public Checkpoint getRecentCheckpoint()
  {
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
  public String getLogicalId()
  {
    return operatorMeta.getName();
  }

  public int getId()
  {
    return id;
  }

  public String getName()
  {
    return name;
  }

  public PhysicalPlan getPlan()
  {
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

  public PTContainer getContainer()
  {
    return container;
  }

  public Map<InputPort<?>, PartitionKeys> getPartitionKeys()
  {
    Map<InputPort<?>, PartitionKeys> pkeys = null;
    if (partitionKeys != null) {
      pkeys = Maps.newHashMapWithExpectedSize(partitionKeys.size());
      for (Map.Entry<InputPortMeta, PartitionKeys> e : partitionKeys.entrySet()) {
        pkeys.put(e.getKey().getPort(), e.getValue());
      }
    }
    return pkeys;
  }

  public int getBufferServerMemory()
  {
    int bufferServerMemory = 0;
    for (int i = 0; i < outputs.size(); i++) {
      if (!outputs.get(i).isDownStreamInline()) {
        bufferServerMemory += outputs.get(i).logicalStream.getSource().getValue(Context.PortContext.BUFFER_MEMORY_MB);
      }
    }
    return bufferServerMemory;
  }

  public Set<PTOperator> getThreadLocalOperators()
  {
    Set<PTOperator> threadLocalOperators = null;
    for (int i = 0; i < outputs.size(); i++) {
      if (outputs.get(i).logicalStream != null && outputs.get(i).logicalStream.getLocality() == Locality.THREAD_LOCAL) {
        if (threadLocalOperators == null) {
          threadLocalOperators = new HashSet<>();
        }
        threadLocalOperators.addAll(outputs.get(i).threadLocalSinks());
      }
    }
    return threadLocalOperators;
  }

  public void setPartitionKeys(Map<InputPort<?>, PartitionKeys> portKeys)
  {
    if (portKeys == null) {
      this.partitionKeys = Collections.emptyMap();
      return;
    }
    HashMap<LogicalPlan.InputPortMeta, PartitionKeys> partitionKeys = Maps.newHashMapWithExpectedSize(portKeys.size());
    for (Map.Entry<InputPort<?>, PartitionKeys> portEntry : portKeys.entrySet()) {
      LogicalPlan.InputPortMeta pportMeta = operatorMeta.getMeta(portEntry.getKey());
      if (pportMeta == null) {
        throw new AssertionError("Invalid port reference " + portEntry);
      }
      partitionKeys.put(pportMeta, portEntry.getValue());
    }
    this.partitionKeys = partitionKeys;
  }

  public boolean isUnifier()
  {
    return unifiedOperatorMeta != null;
  }

  public OperatorMeta getUnifiedOperatorMeta()
  {
    return unifiedOperatorMeta;
  }

  public Class<?> getUnifierClass()
  {
    if (unifiedOperatorMeta == null) {
      throw new IllegalStateException("OperatorMeta does not support this method, please call isUnifier before making this call");
    }

    return operatorMeta.getOperator().getClass();
  }

  public boolean isOperatorStateLess()
  {
    if (operatorMeta.getDAG().getValue(OperatorContext.STATELESS) || operatorMeta.getValue(OperatorContext.STATELESS)) {
      return true;
    }

    return operatorMeta.getOperator().getClass().isAnnotationPresent(Stateless.class);
  }

  public Checkpoint addCheckpoint(long windowId, long startTime)
  {
    int widthMillis = operatorMeta.getDAG().getValue(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS);
    long millis = WindowGenerator.getNextWindowMillis(windowId, startTime, widthMillis);
    long count = WindowGenerator.getWindowCount(millis, startTime, widthMillis);
    Checkpoint c = new Checkpoint(windowId, (int)(count % operatorMeta.getValue(OperatorContext.APPLICATION_WINDOW_COUNT)), (int)(count % operatorMeta.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT)));
    this.checkpoints.add(c);
    return c;
  }

  HostOperatorSet getGrouping(Locality type)
  {
    HostOperatorSet grpObj = this.groupings.get(type);
    if (grpObj == null) {
      grpObj = new HostOperatorSet();
      grpObj.operatorSet = Sets.newHashSet();
      this.groupings.put(type, grpObj);
    }
    return grpObj;
  }

  public HostOperatorSet getNodeLocalOperators()
  {
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
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", id).append("name", name).append("state", state).toString();
  }

}

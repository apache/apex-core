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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.common.util.AsyncStorageAgent;
import org.apache.commons.lang.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.AffinityRule;
import com.datatorrent.api.AffinityRule.Type;
import com.datatorrent.api.AffinityRulesSet;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StatsListener.OperatorRequest;
import com.datatorrent.api.StatsListener.StatsListenerContext;
import com.datatorrent.api.StatsListener.StatsListenerWithContext;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.stram.Journal.Recoverable;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorPair;
import com.datatorrent.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.logical.StreamCodecWrapperForPersistance;
import com.datatorrent.stram.plan.physical.PTOperator.HostOperatorSet;
import com.datatorrent.stram.plan.physical.PTOperator.PTInput;
import com.datatorrent.stram.plan.physical.PTOperator.PTOutput;

/**
 * Translates the logical DAG into physical model. Is the initial query planner
 * and performs dynamic changes.
 * <p>
 * Attributes in the logical DAG affect how the physical plan is derived.
 * Examples include partitioning schemes, resource allocation, recovery
 * semantics etc.<br>
 *
 * The current implementation does not dynamically change or optimize allocation
 * of containers. The maximum number of containers and container size can be
 * specified per application, but all containers are requested at the same size
 * and execution will block until all containers were allocated by the resource
 * manager. Future enhancements will allow to define resource constraints at the
 * operator level and elasticity in resource allocation.<br>
 *
 * @since 0.3.2
 */
public class PhysicalPlan implements Serializable
{
  private static final long serialVersionUID = 201312112033L;
  private static final Logger LOG = LoggerFactory.getLogger(PhysicalPlan.class);

  public static class LoadIndicator
  {
    public final int indicator;
    public final String note;

    LoadIndicator(int indicator, String note)
    {
      this.indicator = indicator;
      this.note = note;
    }
  }

  private final AtomicInteger idSequence = new AtomicInteger();
  final AtomicInteger containerSeq = new AtomicInteger();
  private LinkedHashMap<OperatorMeta, PMapping> logicalToPTOperator = new LinkedHashMap<>();
  private final List<PTContainer> containers = new CopyOnWriteArrayList<>();
  private final LogicalPlan dag;
  private final transient PlanContext ctx;
  private int maxContainers = 1;
  private int availableMemoryMB = Integer.MAX_VALUE;
  private final LocalityPrefs localityPrefs = new LocalityPrefs();
  private final LocalityPrefs inlinePrefs = new LocalityPrefs();

  final Set<PTOperator> deployOpers = Sets.newHashSet();
  final Map<PTOperator, Operator> newOpers = Maps.newHashMap();
  final Set<PTOperator> undeployOpers = Sets.newHashSet();
  final ConcurrentMap<Integer, PTOperator> allOperators = Maps.newConcurrentMap();
  private final ConcurrentMap<OperatorMeta, OperatorMeta> pendingRepartition = Maps.newConcurrentMap();

  private final AtomicInteger strCodecIdSequence = new AtomicInteger();
  private final Map<StreamCodec<?>, Integer> streamCodecIdentifiers = Maps.newHashMap();
  private StatsListenerContext statsListenerContext = new StatsListenerContextImpl();

  private PTContainer getContainer(int index)
  {
    if (index >= containers.size()) {
      if (index >= maxContainers) {
        index = maxContainers - 1;
      }
      for (int i = containers.size(); i < index + 1; i++) {
        containers.add(i, new PTContainer(this));
      }
    }
    return containers.get(index);
  }

  /**
   * Interface to execution context that can be mocked for plan testing.
   */
  public interface PlanContext
  {

    /**
     * Record an event in the event log
     *
     * @param ev The event
     *
     */
    void recordEventAsync(StramEvent ev);

    /**
     * Request deployment change as sequence of undeploy, container start and deploy groups with dependency.
     * Called on initial plan and on dynamic changes during execution.
     * @param releaseContainers
     * @param undeploy
     * @param startContainers
     * @param deploy
     */
    void deploy(Set<PTContainer> releaseContainers, Collection<PTOperator> undeploy, Set<PTContainer> startContainers, Collection<PTOperator> deploy);

    /**
     * Trigger event to perform plan modification.
     * @param r
     */
    void dispatch(Runnable r);

    /**
     * Write the recoverable operation to the log.
     * @param operation
     */
    void writeJournal(Recoverable operation);

    void addOperatorRequest(PTOperator oper, StramToNodeRequest request);
  }

  /**
   * Adapter for handling operator implementing StatsListener interface.
   */
  private static class StatsListenerProxy implements StatsListenerWithContext, Serializable
  {
    private static final long serialVersionUID = 201312112033L;
    private final OperatorMeta om;

    private StatsListenerProxy(OperatorMeta om)
    {
      this.om = om;
    }

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      return ((StatsListener)om.getOperator()).processStats(stats);
    }

    @Override
    public Response processStats(BatchedOperatorStats stats, StatsListenerContext context)
    {
      StatsListener listener = (StatsListener)om.getOperator();
      if (listener instanceof StatsListenerWithContext) {
        return ((StatsListenerWithContext)listener).processStats(stats, context);
      } else {
        return processStats(stats);
      }
    }
  }

  /**
   * Adapter for handling deprecated StatsListener. This implementation calls {@link StatsListener#processStats(BatchedOperatorStats)}
   * of the old stats listener ignoring context.
   */
  private static class StatsListenerAdapterForStatsListener implements  StatsListener.StatsListenerWithContext, Serializable
  {
    private static final long serialVersionUID = 201312112345033L;
    private final StatsListener listener;

    private StatsListenerAdapterForStatsListener(StatsListener listener)
    {
      this.listener = listener;
    }

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      return listener.processStats(stats);
    }

    @Override
    public Response processStats(BatchedOperatorStats stats, StatsListenerContext context)
    {
      return listener.processStats(stats);
    }
  }

  private static StatsListenerWithContext getStatsListenerAdapter(StatsListener listener)
  {
    if (listener instanceof StatsListenerWithContext) {
      return (StatsListenerWithContext)listener;
    } else {
      return new StatsListenerAdapterForStatsListener(listener);
    }
  }

  /**
   * The logical operator with physical plan info tagged on.
   */
  public static class PMapping implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312112033L;

    private final OperatorMeta logicalOperator;
    private List<PTOperator> partitions = new LinkedList<>();
    private final Map<LogicalPlan.OutputPortMeta, StreamMapping> outputStreams = Maps.newHashMap();
    private List<StatsListenerWithContext> statsHandlers;

    /**
     * Operators that form a parallel partition
     */
    private Set<OperatorMeta> parallelPartitions = Sets.newHashSet();

    private PMapping(OperatorMeta om)
    {
      this.logicalOperator = om;
    }

    private void addPartition(PTOperator p)
    {
      partitions.add(p);
      p.statsListeners = this.statsHandlers;
    }

    /**
     * Return all partitions and unifiers, except MxN unifiers
     * @return
     */
    private Collection<PTOperator> getAllOperators()
    {
      Collection<PTOperator> c = new ArrayList<>(partitions.size() + 1);
      c.addAll(partitions);
      for (StreamMapping ug : outputStreams.values()) {
        ug.addTo(c);
      }
      return c;
    }

    @Override
    public String toString()
    {
      return logicalOperator.toString();
    }
  }

  private class LocalityPref implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312112033L;
    String host;
    Set<PMapping> operators = Sets.newHashSet();
  }

  /**
   * Group logical operators by locality constraint. Used to derive locality
   * groupings for physical operators, which are used when assigning containers
   * and requesting resources from the scheduler.
   */
  private class LocalityPrefs implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312112033L;
    private final Map<PMapping, LocalityPref> prefs = Maps.newHashMap();
    private final AtomicInteger groupSeq = new AtomicInteger();

    void add(PMapping m, String group)
    {
      if (group != null) {
        LocalityPref pref = null;
        for (LocalityPref lp : prefs.values()) {
          if (group.equals(lp.host)) {
            lp.operators.add(m);
            pref = lp;
            break;
          }
        }
        if (pref == null) {
          pref = new LocalityPref();
          pref.host = group;
          pref.operators.add(m);
          this.prefs.put(m, pref);
        }
      }
    }

    // if netbeans is not smart, don't produce warnings in other IDE
    //@SuppressWarnings("null") /* for lp2.operators.add(m1); line below - netbeans is not very smart; you don't be an idiot! */
    void setLocal(PMapping m1, PMapping m2)
    {
      LocalityPref lp1 = prefs.get(m1);
      LocalityPref lp2 = prefs.get(m2);

      if (lp1 == null && lp2 == null) {
        lp1 = lp2 = new LocalityPref();
        lp1.host = "host" + groupSeq.incrementAndGet();
        lp1.operators.add(m1);
        lp1.operators.add(m2);
      } else if (lp1 != null && lp2 != null) {
        // check if we can combine
        if (StringUtils.equals(lp1.host, lp2.host)) {
          lp1.operators.addAll(lp2.operators);
          lp2.operators.addAll(lp1.operators);
        } else {
          LOG.warn("Node locality conflict {} {}", m1, m2);
        }
      } else {
        if (lp1 == null) {
          lp2.operators.add(m1);
          lp1 = lp2;
        } else {
          lp1.operators.add(m2);
          lp2 = lp1;
        }
      }

      prefs.put(m1, lp1);
      prefs.put(m2, lp2);
    }

  }

  /**
   *
   * @param dag
   * @param ctx
   */
  public PhysicalPlan(LogicalPlan dag, PlanContext ctx)
  {

    this.dag = dag;
    this.ctx = ctx;
    this.maxContainers = Math.max(dag.getMaxContainerCount(), 1);
    LOG.debug("Max containers: {}", this.maxContainers);

    Stack<OperatorMeta> pendingNodes = new Stack<>();

    // Add logging operators for streams if not added already
    updatePersistOperatorStreamCodec(dag);

    for (OperatorMeta n : dag.getAllOperators()) {
      pendingNodes.push(n);
    }

    while (!pendingNodes.isEmpty()) {
      OperatorMeta n = pendingNodes.pop();

      if (this.logicalToPTOperator.containsKey(n)) {
        // already processed as upstream dependency
        continue;
      }

      boolean upstreamDeployed = true;

      for (Map.Entry<InputPortMeta, StreamMeta> entry : n.getInputStreams().entrySet()) {
        StreamMeta s = entry.getValue();
        boolean delay = entry.getKey().getValue(LogicalPlan.IS_CONNECTED_TO_DELAY_OPERATOR);
        // skip delay sources since it's going to be handled as downstream
        if (!delay && s.getSource() != null && !this.logicalToPTOperator.containsKey(s.getSource().getOperatorMeta())) {
          pendingNodes.push(n);
          pendingNodes.push(s.getSource().getOperatorMeta());
          upstreamDeployed = false;
          break;
        }
      }

      if (upstreamDeployed) {
        addLogicalOperator(n);
      }
    }

    // Add inlinePrefs and localityPrefs for affinity rules
    AffinityRulesSet affinityRuleSet = dag.getAttributes().get(DAGContext.AFFINITY_RULES_SET);
    if (affinityRuleSet != null && affinityRuleSet.getAffinityRules() != null) {
      for (AffinityRule rule : affinityRuleSet.getAffinityRules()) {
        if (rule.getOperatorsList() != null) {
          for (int i = 0; i < rule.getOperatorsList().size() - 1; i++) {
            for (int j = i + 1; j < rule.getOperatorsList().size(); j++) {
              OperatorPair operators = new OperatorPair(rule.getOperatorsList().get(i), rule.getOperatorsList().get(j));
              PMapping firstPMapping = logicalToPTOperator.get(dag.getOperatorMeta(operators.first));
              OperatorMeta opMeta = dag.getOperatorMeta(operators.second);
              PMapping secondMapping = logicalToPTOperator.get(opMeta);

              if (rule.getType() == Type.AFFINITY) {
                // ONLY node and container mappings are supported right now
                if (Locality.CONTAINER_LOCAL == rule.getLocality()) {
                  inlinePrefs.setLocal(firstPMapping, secondMapping);
                } else if (Locality.NODE_LOCAL == rule.getLocality()) {
                  localityPrefs.setLocal(firstPMapping, secondMapping);
                }
                for (PTOperator ptOperator : firstPMapping.partitions) {
                  setLocalityGrouping(firstPMapping, ptOperator, inlinePrefs, Locality.CONTAINER_LOCAL, null);
                  setLocalityGrouping(firstPMapping, ptOperator, localityPrefs, Locality.NODE_LOCAL, null);
                }
              }
            }
          }
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      // Log group set for all PT Operators
      for (OperatorMeta operator : dag.getAllOperators()) {
        PMapping mapping = logicalToPTOperator.get(operator);
        if (mapping != null) {
          for (PTOperator ptOperaror : mapping.partitions) {
            List<String> operators = new ArrayList<>();
            for (PTOperator op : ptOperaror.getGrouping(Locality.CONTAINER_LOCAL).getOperatorSet()) {
              operators.add(op.getLogicalId());
            }
            LOG.debug("Operator {} Partition {} CONTAINER LOCAL Operator set  = {}", operator.getName(), ptOperaror.id, StringUtils.join(operators, ","));

            operators.clear();
            for (PTOperator op : ptOperaror.getGrouping(Locality.NODE_LOCAL).getOperatorSet()) {
              operators.add(op.getLogicalId());
            }
            LOG.debug("Operator {} Partition {} NODE LOCAL Operator set  = {}", operator.getName(), ptOperaror.id, StringUtils.join(operators, ","));

          }
        }
      }
    }

    updatePartitionsInfoForPersistOperator(dag);

    Map<PTOperator, PTContainer> operatorContainerMap = new HashMap<>();

    // assign operators to containers
    int groupCount = 0;
    Set<PTOperator> deployOperators = Sets.newHashSet();
    for (Map.Entry<OperatorMeta, PMapping> e : logicalToPTOperator.entrySet()) {
      for (PTOperator oper : e.getValue().getAllOperators()) {
        if (oper.container == null) {
          PTContainer container = getContainer((groupCount++) % maxContainers);
          if (!container.operators.isEmpty()) {
            LOG.warn("Operator {} shares container without locality contraint due to insufficient resources.", oper);
          }

          Set<PTOperator> inlineSet = oper.getGrouping(Locality.CONTAINER_LOCAL).getOperatorSet();
          if (!inlineSet.isEmpty()) {
            // process inline operators
            for (PTOperator inlineOper : inlineSet) {
              setContainer(inlineOper, container);
              operatorContainerMap.put(inlineOper, container);
            }
          } else {
            setContainer(oper, container);
          }
          operatorContainerMap.put(oper, container);
          deployOperators.addAll(container.operators);
        }
      }
    }


    for (PTContainer container : containers) {
      updateContainerMemoryWithBufferServer(container);
      container.setRequiredVCores(getVCores(container.getOperators()));
    }

    // Add anti-affinity restrictions in Containers
    if (affinityRuleSet != null && affinityRuleSet.getAffinityRules() != null) {
      setAntiAffinityForContainers(dag, affinityRuleSet.getAffinityRules(), operatorContainerMap);
    }

    // Log container anti-affinity
    if (LOG.isDebugEnabled()) {
      for (PTContainer container : containers) {
        List<String> antiOperators = new ArrayList<>();
        for (PTContainer c : container.getStrictAntiPrefs()) {
          for (PTOperator operator : c.getOperators()) {
            antiOperators.add(operator.getName());
          }
        }
        List<String> containerOperators = new ArrayList<>();
        for (PTOperator operator : container.getOperators()) {
          containerOperators.add(operator.getName());
        }
        LOG.debug("Container with operators [{}] has anti affinity with [{}]", StringUtils.join(containerOperators, ","), StringUtils.join(antiOperators, ","));
      }
    }

    for (Map.Entry<PTOperator, Operator> operEntry : this.newOpers.entrySet()) {
      initCheckpoint(operEntry.getKey(), operEntry.getValue(), Checkpoint.INITIAL_CHECKPOINT);
    }
    // request initial deployment
    ctx.deploy(Collections.<PTContainer>emptySet(), Collections.<PTOperator>emptySet(), Sets.newHashSet(containers), deployOperators);
    this.newOpers.clear();
    this.deployOpers.clear();
    this.undeployOpers.clear();
  }

  public void setAntiAffinityForContainers(LogicalPlan dag, Collection<AffinityRule> affinityRules, Map<PTOperator, PTContainer> operatorContainerMap)
  {
    for (AffinityRule rule : affinityRules) {
      if (rule.getOperatorsList() != null && rule.getType() == Type.ANTI_AFFINITY) {
        for (int i = 0; i < rule.getOperatorsList().size() - 1; i++) {
          for (int j = i + 1; j < rule.getOperatorsList().size(); j++) {
            OperatorPair operators = new OperatorPair(rule.getOperatorsList().get(i), rule.getOperatorsList().get(j));

            PMapping firstPMapping = logicalToPTOperator.get(dag.getOperatorMeta(operators.first));
            OperatorMeta opMeta = dag.getOperatorMeta(operators.second);
            PMapping secondMapping = logicalToPTOperator.get(opMeta);
            for (PTOperator firstPtOperator : firstPMapping.partitions) {
              PTContainer firstContainer = operatorContainerMap.get(firstPtOperator);
              for (PTOperator secondPtOperator : secondMapping.partitions) {
                PTContainer secondContainer = operatorContainerMap.get(secondPtOperator);
                if (firstContainer == secondContainer || firstContainer.getStrictAntiPrefs().contains(secondContainer)) {
                  continue;
                }
                if (rule.isRelaxLocality()) {
                  firstContainer.getPreferredAntiPrefs().add(secondContainer);
                  secondContainer.getPreferredAntiPrefs().add(firstContainer);
                } else {
                  firstContainer.getStrictAntiPrefs().add(secondContainer);
                  secondContainer.getStrictAntiPrefs().add(firstContainer);
                }
              }
            }
          }
        }
      }
    }
  }

  private void updatePartitionsInfoForPersistOperator(LogicalPlan dag)
  {
    // Add Partition mask and partition keys of Sinks to persist to Wrapper
    // StreamCodec for persist operator
    try {
      for (OperatorMeta n : dag.getAllOperators()) {
        for (StreamMeta s : n.getOutputStreams().values()) {
          if (s.getPersistOperator() != null) {
            InputPortMeta persistInputPort = s.getPersistOperatorInputPort();
            StreamCodecWrapperForPersistance<?> persistCodec = (StreamCodecWrapperForPersistance<?>)persistInputPort.getStreamCodec();
            if (persistCodec == null) {
              continue;
            }
            // Logging is enabled for the stream
            for (InputPortMeta portMeta : s.getSinksToPersist()) {
              updatePersistOperatorWithSinkPartitions(persistInputPort, s.getPersistOperator(), persistCodec, portMeta);
            }
          }

          // Check partitioning for persist operators per sink too
          for (Map.Entry<InputPortMeta, InputPortMeta> entry : s.sinkSpecificPersistInputPortMap.entrySet()) {
            InputPortMeta persistInputPort = entry.getValue();
            StreamCodec<?> streamCodec = persistInputPort.getStreamCodec();
            if (streamCodec != null && streamCodec instanceof StreamCodecWrapperForPersistance) {
              updatePersistOperatorWithSinkPartitions(persistInputPort, s.sinkSpecificPersistOperatorMap.get(entry.getKey()), (StreamCodecWrapperForPersistance<?>)streamCodec, entry.getKey());
            }
          }
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void updatePersistOperatorWithSinkPartitions(InputPortMeta persistInputPort, OperatorMeta persistOperatorMeta, StreamCodecWrapperForPersistance<?> persistCodec, InputPortMeta sinkPortMeta)
  {
    Collection<PTOperator> ptOperators = getOperators(sinkPortMeta.getOperatorMeta());
    Collection<PartitionKeys> partitionKeysList = new ArrayList<>();
    for (PTOperator p : ptOperators) {
      PartitionKeys keys = p.partitionKeys.get(sinkPortMeta);
      partitionKeysList.add(keys);
    }

    persistCodec.inputPortToPartitionMap.put(sinkPortMeta, partitionKeysList);
  }

  private void updatePersistOperatorStreamCodec(LogicalPlan dag)
  {
    HashMap<StreamMeta, StreamCodec<?>> streamMetaToCodecMap = new HashMap<>();
    try {
      for (OperatorMeta n : dag.getAllOperators()) {
        for (StreamMeta s : n.getOutputStreams().values()) {
          if (s.getPersistOperator() != null) {
            Map<InputPortMeta, StreamCodec<?>> inputStreamCodecs = new HashMap<>();
            // Logging is enabled for the stream
            for (InputPortMeta portMeta : s.getSinksToPersist()) {
              StreamCodec<?> inputStreamCodec = portMeta.getStreamCodec();
              if (inputStreamCodec != null) {
                boolean alreadyAdded = false;

                for (StreamCodec<?> codec : inputStreamCodecs.values()) {
                  if (inputStreamCodec.equals(codec)) {
                    alreadyAdded = true;
                    break;
                  }
                }
                if (!alreadyAdded) {
                  inputStreamCodecs.put(portMeta, inputStreamCodec);
                }
              }
            }

            if (inputStreamCodecs.isEmpty()) {
              // Stream codec not specified
              // So everything out of Source should be captured without any
              // StreamCodec
              // Do nothing
            } else {
              // Create Wrapper codec for Stream persistence using all unique
              // stream codecs
              // Logger should write merged or union of all input stream codecs
              StreamCodec<?> specifiedCodecForLogger = s.getPersistOperatorInputPort().getStreamCodec();
              @SuppressWarnings({ "unchecked", "rawtypes" })
              StreamCodecWrapperForPersistance<Object> codec = new StreamCodecWrapperForPersistance(inputStreamCodecs, specifiedCodecForLogger);
              streamMetaToCodecMap.put(s, codec);
            }
          }
        }
      }

      for (java.util.Map.Entry<StreamMeta, StreamCodec<?>> entry : streamMetaToCodecMap.entrySet()) {
        dag.setInputPortAttribute(entry.getKey().getPersistOperatorInputPort().getPort(), PortContext.STREAM_CODEC, entry.getValue());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void setContainer(PTOperator pOperator, PTContainer container)
  {
    LOG.debug("Setting container {} for {}", container, pOperator);
    assert (pOperator.container == null) : "Container already assigned for " + pOperator;
    pOperator.container = container;
    container.operators.add(pOperator);
    int upStreamUnifierMemory = 0;
    if (!pOperator.upstreamMerge.isEmpty()) {
      for (Map.Entry<InputPortMeta, PTOperator> mEntry : pOperator.upstreamMerge.entrySet()) {
        assert (mEntry.getValue().container == null) : "Container already assigned for " + mEntry.getValue();
        mEntry.getValue().container = container;
        container.operators.add(mEntry.getValue());
        upStreamUnifierMemory += mEntry.getValue().getOperatorMeta().getValue(OperatorContext.MEMORY_MB);
      }
    }
    int memoryMB = pOperator.getOperatorMeta().getValue(OperatorContext.MEMORY_MB) + upStreamUnifierMemory;
    container.setRequiredMemoryMB(container.getRequiredMemoryMB() + memoryMB);
  }

  private void updateContainerMemoryWithBufferServer(PTContainer container)
  {
    int bufferServerMemory = 0;
    for (PTOperator operator : container.getOperators()) {
      bufferServerMemory += operator.getBufferServerMemory();
    }
    container.setRequiredMemoryMB(container.getRequiredMemoryMB() + bufferServerMemory);
  }

  /**
   * This returns the vCores for a set of operators in a container. This forms the group of thread_local operators and get the maximum value of the group
   *
   * @param operators The container local operators
   * @return the number of vcores required for a container
   */
  private int getVCores(Collection<PTOperator> operators)
  {
    // this forms the groups of thread local operators in the given container
    HashMap<PTOperator, Set<PTOperator>> groupMap = new HashMap<>();
    for (PTOperator operator : operators) {
      Set<PTOperator> group = new HashSet<>();
      group.add(operator);
      groupMap.put(operator, group);
    }
    int vCores = 0;
    for (PTOperator operator : operators) {
      Set<PTOperator> threadLocal = operator.getThreadLocalOperators();
      if (threadLocal != null) {
        Set<PTOperator> group = groupMap.get(operator);
        for (PTOperator operator1 : threadLocal) {
          group.addAll(groupMap.get(operator1));
        }
        for (PTOperator operator1 : group) {
          groupMap.put(operator1, group);
        }
      }
    }
    Set<PTOperator> visitedOperators = new HashSet<>();
    for (Map.Entry<PTOperator, Set<PTOperator>> group : groupMap.entrySet()) {
      if (!visitedOperators.contains(group.getKey())) {
        visitedOperators.addAll(group.getValue());
        int tempCores = 0;
        for (PTOperator operator : group.getValue()) {
          tempCores = Math.max(tempCores, operator.getOperatorMeta().getValue(OperatorContext.VCORES));
        }
        vCores += tempCores;
      }
    }
    return vCores;
  }

  private class PartitioningContextImpl implements Partitioner.PartitioningContext
  {
    private List<InputPort<?>> inputPorts;
    private final int parallelPartitionCount;
    private final PMapping om;

    private PartitioningContextImpl(PMapping om, int parallelPartitionCount)
    {
      this.om = om;
      this.parallelPartitionCount = parallelPartitionCount;
    }

    @Override
    public int getParallelPartitionCount()
    {
      return parallelPartitionCount;
    }

    @Override
    public List<InputPort<?>> getInputPorts()
    {
      if (inputPorts == null) {
        inputPorts = getInputPortList(om.logicalOperator);
      }
      return inputPorts;
    }
  }

  private void initPartitioning(PMapping m, int partitionCnt)
  {
    Operator operator = m.logicalOperator.getOperator();
    Collection<Partition<Operator>> partitions;

    @SuppressWarnings("unchecked")
    Partitioner<Operator> partitioner = m.logicalOperator.getAttributes().contains(OperatorContext.PARTITIONER)
        ? (Partitioner<Operator>)m.logicalOperator.getValue(OperatorContext.PARTITIONER)
        : operator instanceof Partitioner ? (Partitioner<Operator>)operator : null;

    Collection<Partition<Operator>> collection = new ArrayList<>(1);
    DefaultPartition<Operator> firstPartition = new DefaultPartition<>(operator);
    collection.add(firstPartition);

    if (partitioner != null) {
      partitions = partitioner.definePartitions(collection, new PartitioningContextImpl(m, partitionCnt));

      if (partitions == null || partitions.isEmpty()) {
        throw new IllegalStateException("Partitioner returns null or empty.");
      }
    } else {
      //This handles the case when parallel partitioning is occurring. Partition count will be
      //Non zero in the case of parallel partitioning.
      for (int partitionCounter = 0; partitionCounter < partitionCnt - 1; partitionCounter++) {
        collection.add(firstPartition);
      }
      partitions = collection;
    }

    Collection<StatsListener> statsListeners = m.logicalOperator.getValue(OperatorContext.STATS_LISTENERS);
    if (statsListeners != null && !statsListeners.isEmpty()) {
      if (m.statsHandlers == null) {
        m.statsHandlers = new ArrayList<>(statsListeners.size());
      }
      for (StatsListener sl : statsListeners) {
        m.statsHandlers.add(getStatsListenerAdapter(sl));
      }
    }

    if (m.logicalOperator.getOperator() instanceof StatsListener) {
      if (m.statsHandlers == null) {
        m.statsHandlers = new ArrayList<>(1);
      }
      m.statsHandlers.add(new StatsListenerProxy(m.logicalOperator));
    }

    // create operator instance per partition
    Map<Integer, Partition<Operator>> operatorIdToPartition = Maps.newHashMapWithExpectedSize(partitions.size());
    for (Partition<Operator> partition : partitions) {
      PTOperator p = addPTOperator(m, partition, null);
      operatorIdToPartition.put(p.getId(), partition);
    }

    if (partitioner != null) {
      partitioner.partitioned(operatorIdToPartition);
    }
  }

  private class RepartitionContext extends PartitioningContextImpl
  {
    final List<PTOperator> operators;
    final List<DefaultPartition<Operator>> currentPartitions;
    final Map<Partition<?>, PTOperator> currentPartitionMap;
    final Map<Integer, Partition<Operator>> operatorIdToPartition;
    final List<Partition<Operator>> addedPartitions = new ArrayList<>();
    Checkpoint minCheckpoint = null;
    Collection<Partition<Operator>> newPartitions = null;

    RepartitionContext(Partitioner<Operator> partitioner, PMapping currentMapping, int partitionCount)
    {
      super(currentMapping, partitionCount);
      this.operators = currentMapping.partitions;
      this.currentPartitions = new ArrayList<>(operators.size());
      this.currentPartitionMap = Maps.newHashMapWithExpectedSize(operators.size());
      this.operatorIdToPartition = Maps.newHashMapWithExpectedSize(operators.size());

      // collect current partitions with committed operator state
      // those will be needed by the partitioner for split/merge
      for (PTOperator pOperator : operators) {
        Map<InputPort<?>, PartitionKeys> pks = pOperator.getPartitionKeys();
        if (pks == null) {
          throw new AssertionError("Null partition: " + pOperator);
        }

        // if partitions checkpoint at different windows, processing for new or modified
        // partitions will start from earliest checkpoint found (at least once semantics)
        if (minCheckpoint == null) {
          minCheckpoint = pOperator.recoveryCheckpoint;
        } else if (minCheckpoint.windowId > pOperator.recoveryCheckpoint.windowId) {
          minCheckpoint = pOperator.recoveryCheckpoint;
        }

        Operator partitionedOperator = loadOperator(pOperator);
        DefaultPartition<Operator> partition = new DefaultPartition<>(partitionedOperator, pks, pOperator.loadIndicator, pOperator.stats);
        currentPartitions.add(partition);
        currentPartitionMap.put(partition, pOperator);
        LOG.debug("partition load: {} {} {}", pOperator, partition.getPartitionKeys(), partition.getLoad());
        operatorIdToPartition.put(pOperator.getId(), partition);
      }

      newPartitions = partitioner.definePartitions(new ArrayList<Partition<Operator>>(currentPartitions), this);
    }

  }

  private Partitioner<Operator> getPartitioner(PMapping currentMapping)
  {
    Operator operator = currentMapping.logicalOperator.getOperator();
    Partitioner<Operator> partitioner = null;
    if (currentMapping.logicalOperator.getAttributes().contains(OperatorContext.PARTITIONER)) {
      @SuppressWarnings("unchecked")
      Partitioner<Operator> tmp = (Partitioner<Operator>)currentMapping.logicalOperator.getValue(OperatorContext.PARTITIONER);
      partitioner = tmp;
    } else if (operator instanceof Partitioner) {
      @SuppressWarnings("unchecked")
      Partitioner<Operator> tmp = (Partitioner<Operator>)operator;
      partitioner = tmp;
    }
    return partitioner;
  }

  private void redoPartitions(PMapping currentMapping, String note)
  {
    Partitioner<Operator> partitioner = getPartitioner(currentMapping);
    if (partitioner == null) {
      LOG.warn("No partitioner for {}", currentMapping.logicalOperator);
      return;
    }

    RepartitionContext mainPC = new RepartitionContext(partitioner, currentMapping, 0);
    if (mainPC.newPartitions.isEmpty()) {
      LOG.warn("Empty partition list after repartition: {}", currentMapping.logicalOperator);
      return;
    }

    int memoryPerPartition = currentMapping.logicalOperator.getValue(OperatorContext.MEMORY_MB);
    for (Map.Entry<OutputPortMeta, StreamMeta> stream : currentMapping.logicalOperator.getOutputStreams().entrySet()) {
      if (stream.getValue().getLocality() != Locality.THREAD_LOCAL && stream.getValue().getLocality() != Locality.CONTAINER_LOCAL) {
        memoryPerPartition += stream.getKey().getValue(PortContext.BUFFER_MEMORY_MB);
      }
    }
    for (OperatorMeta pp : currentMapping.parallelPartitions) {
      for (Map.Entry<OutputPortMeta, StreamMeta> stream : pp.getOutputStreams().entrySet()) {
        if (stream.getValue().getLocality() != Locality.THREAD_LOCAL && stream.getValue().getLocality() != Locality.CONTAINER_LOCAL) {
          memoryPerPartition += stream.getKey().getValue(PortContext.BUFFER_MEMORY_MB);
        }
      }
      memoryPerPartition += pp.getValue(OperatorContext.MEMORY_MB);
    }
    int requiredMemoryMB = (mainPC.newPartitions.size() - mainPC.currentPartitions.size()) * memoryPerPartition;
    if (requiredMemoryMB > availableMemoryMB) {
      LOG.warn("Insufficient headroom for repartitioning: available {}m required {}m", availableMemoryMB, requiredMemoryMB);
      return;
    }

    List<Partition<Operator>> addedPartitions = new ArrayList<>();
    // determine modifications of partition set, identify affected operator instance(s)
    for (Partition<Operator> newPartition : mainPC.newPartitions) {
      PTOperator op = mainPC.currentPartitionMap.remove(newPartition);
      if (op == null) {
        addedPartitions.add(newPartition);
      } else {
        // check whether mapping was changed
        for (DefaultPartition<Operator> pi : mainPC.currentPartitions) {
          if (pi == newPartition && pi.isModified()) {
            // existing partition changed (operator or partition keys)
            // remove/add to update subscribers and state
            mainPC.currentPartitionMap.put(newPartition, op);
            addedPartitions.add(newPartition);
          }
        }
      }
    }

    // remaining entries represent deprecated partitions
    this.undeployOpers.addAll(mainPC.currentPartitionMap.values());
    // downstream dependencies require redeploy, resolve prior to modifying plan
    Set<PTOperator> deps = this.getDependents(mainPC.currentPartitionMap.values());
    this.undeployOpers.addAll(deps);
    // dependencies need redeploy, except operators excluded in remove
    this.deployOpers.addAll(deps);

    // process parallel partitions before removing operators from the plan
    LinkedHashMap<PMapping, RepartitionContext> partitionContexts = Maps.newLinkedHashMap();
    Stack<OperatorMeta> parallelPartitions = new Stack<>();
    parallelPartitions.addAll(currentMapping.parallelPartitions);
    pendingLoop:
    while (!parallelPartitions.isEmpty()) {
      OperatorMeta ppMeta = parallelPartitions.pop();
      for (StreamMeta s : ppMeta.getInputStreams().values()) {
        if (currentMapping.parallelPartitions.contains(s.getSource().getOperatorMeta()) && parallelPartitions.contains(s.getSource().getOperatorMeta())) {
          parallelPartitions.push(ppMeta);
          parallelPartitions.remove(s.getSource().getOperatorMeta());
          parallelPartitions.push(s.getSource().getOperatorMeta());
          continue pendingLoop;
        }
      }
      LOG.debug("Processing parallel partition {}", ppMeta);

      PMapping ppm = this.logicalToPTOperator.get(ppMeta);
      Partitioner<Operator> ppp = getPartitioner(ppm);
      if (ppp == null) {
        partitionContexts.put(ppm, null);
      } else {
        RepartitionContext pc = new RepartitionContext(ppp, ppm, mainPC.newPartitions.size());
        if (pc.newPartitions == null) {
          throw new IllegalStateException("Partitioner returns null for parallel partition " + ppm.logicalOperator);
        }
        partitionContexts.put(ppm, pc);
      }
    }

    // plan updates start here, after all changes were identified
    // remove obsolete operators first, any freed resources
    // can subsequently be used for new/modified partitions
    List<PTOperator> copyPartitions = Lists.newArrayList(currentMapping.partitions);
    // remove deprecated partitions from plan
    for (PTOperator p : mainPC.currentPartitionMap.values()) {
      copyPartitions.remove(p);
      removePartition(p, currentMapping);
      mainPC.operatorIdToPartition.remove(p.getId());
    }
    currentMapping.partitions = copyPartitions;

    // add new operators
    for (Partition<Operator> newPartition : addedPartitions) {
      PTOperator p = addPTOperator(currentMapping, newPartition, mainPC.minCheckpoint);
      mainPC.operatorIdToPartition.put(p.getId(), newPartition);
    }

    // process parallel partition changes
    for (Map.Entry<PMapping, RepartitionContext> e : partitionContexts.entrySet()) {
      if (e.getValue() == null) {
        // no partitioner, add required operators
        for (int i = 0; i < addedPartitions.size(); i++) {
          LOG.debug("Automatically adding to parallel partition {}", e.getKey());
          // set activation windowId to confirm to upstream checkpoints
          addPTOperator(e.getKey(), null, mainPC.minCheckpoint);
        }
      } else {
        RepartitionContext pc = e.getValue();
        // track previous parallel partition mapping
        Map<Partition<Operator>, Partition<Operator>> prevMapping = Maps.newHashMap();
        for (int i = 0; i < mainPC.currentPartitions.size(); i++) {
          prevMapping.put(pc.currentPartitions.get(i), mainPC.currentPartitions.get(i));
        }
        // determine which new partitions match upstream, remaining to be treated as new operator
        Map<Partition<Operator>, Partition<Operator>> newMapping = Maps.newHashMap();
        Iterator<Partition<Operator>> itMain = mainPC.newPartitions.iterator();
        Iterator<Partition<Operator>> itParallel = pc.newPartitions.iterator();
        while (itMain.hasNext() && itParallel.hasNext()) {
          newMapping.put(itParallel.next(), itMain.next());
        }

        for (Partition<Operator> newPartition : pc.newPartitions) {
          PTOperator op = pc.currentPartitionMap.remove(newPartition);
          if (op == null) {
            pc.addedPartitions.add(newPartition);
          } else if (prevMapping.get(newPartition) != newMapping.get(newPartition)) {
            // upstream partitions don't match, remove/add to replace with new operator
            pc.currentPartitionMap.put(newPartition, op);
            pc.addedPartitions.add(newPartition);
          } else {
            // check whether mapping was changed - based on DefaultPartition implementation
            for (DefaultPartition<Operator> pi : pc.currentPartitions) {
              if (pi == newPartition && pi.isModified()) {
                // existing partition changed (operator or partition keys)
                // remove/add to update subscribers and state
                mainPC.currentPartitionMap.put(newPartition, op);
                pc.addedPartitions.add(newPartition);
              }
            }
          }
        }

        if (!pc.currentPartitionMap.isEmpty()) {
          // remove obsolete partitions
          List<PTOperator> cowPartitions = Lists.newArrayList(e.getKey().partitions);
          for (PTOperator p : pc.currentPartitionMap.values()) {
            cowPartitions.remove(p);
            removePartition(p, e.getKey());
            pc.operatorIdToPartition.remove(p.getId());
          }
          e.getKey().partitions = cowPartitions;
        }
        // add new partitions
        for (Partition<Operator> newPartition : pc.addedPartitions) {
          PTOperator oper = addPTOperator(e.getKey(), newPartition, mainPC.minCheckpoint);
          pc.operatorIdToPartition.put(oper.getId(), newPartition);
        }

        getPartitioner(e.getKey()).partitioned(pc.operatorIdToPartition);
      }
    }

    updateStreamMappings(currentMapping);
    for (PMapping pp : partitionContexts.keySet()) {
      updateStreamMappings(pp);
    }

    deployChanges();

    if (mainPC.currentPartitions.size() != mainPC.newPartitions.size()) {
      StramEvent ev = new StramEvent.PartitionEvent(currentMapping.logicalOperator.getName(), mainPC.currentPartitions.size(), mainPC.newPartitions.size());
      ev.setReason(note);
      this.ctx.recordEventAsync(ev);
    }

    partitioner.partitioned(mainPC.operatorIdToPartition);
  }

  private void updateStreamMappings(PMapping m)
  {
    for (Map.Entry<OutputPortMeta, StreamMeta> opm : m.logicalOperator.getOutputStreams().entrySet()) {
      StreamMapping ug = m.outputStreams.get(opm.getKey());
      if (ug == null) {
        ug = new StreamMapping(opm.getValue(), this);
        m.outputStreams.put(opm.getKey(), ug);
      }
      LOG.debug("update stream mapping for {} {}", opm.getKey().getOperatorMeta(), opm.getKey().getPortName());
      ug.setSources(m.partitions);
    }

    for (Map.Entry<InputPortMeta, StreamMeta> ipm : m.logicalOperator.getInputStreams().entrySet()) {
      PMapping sourceMapping = this.logicalToPTOperator.get(ipm.getValue().getSource().getOperatorMeta());
      if (ipm.getValue().getSource().getOperatorMeta().getOperator() instanceof Operator.DelayOperator) {
        // skip if the source is a DelayOperator
        continue;
      }
      if (ipm.getKey().getValue(PortContext.PARTITION_PARALLEL)) {
        if (sourceMapping.partitions.size() < m.partitions.size()) {
          throw new AssertionError("Number of partitions don't match in parallel mapping " + sourceMapping.logicalOperator.getName() + " -> " + m.logicalOperator.getName() + ", " + sourceMapping.partitions.size() + " -> " + m.partitions.size());
        }
        int slidingWindowCount = 0;
        OperatorMeta sourceOM = sourceMapping.logicalOperator;
        if (sourceOM.getAttributes().contains(Context.OperatorContext.SLIDE_BY_WINDOW_COUNT)) {
          if (sourceOM.getValue(Context.OperatorContext.SLIDE_BY_WINDOW_COUNT) <
              sourceOM.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)) {
            slidingWindowCount = sourceOM.getValue(OperatorContext.SLIDE_BY_WINDOW_COUNT);
          } else {
            LOG.warn("Sliding Window Count {} should be less than APPLICATION WINDOW COUNT {}", sourceOM.getValue(Context.OperatorContext.SLIDE_BY_WINDOW_COUNT), sourceOM.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT));
          }
        }
        for (int i = 0; i < m.partitions.size(); i++) {
          PTOperator oper = m.partitions.get(i);
          PTOperator sourceOper = sourceMapping.partitions.get(i);
          for (PTOutput sourceOut : sourceOper.outputs) {
            nextSource:
            if (sourceOut.logicalStream == ipm.getValue()) {
              //avoid duplicate entries in case of parallel partitions
              for (PTInput sinkIn : sourceOut.sinks) {
                //check if the operator is already in the sinks list and also the port name of that sink is same as the
                // input-port-meta currently being looked at since we allow an output port to connect to multiple inputs of the same operator.
                if (sinkIn.target == oper && sinkIn.portName.equals(ipm.getKey().getPortName())) {
                  break nextSource;
                }
              }
              PTInput input;
              if (slidingWindowCount > 0) {
                PTOperator slidingUnifier = StreamMapping.createSlidingUnifier(sourceOut.logicalStream, this,
                    sourceOM.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT), slidingWindowCount);
                StreamMapping.addInput(slidingUnifier, sourceOut, null);
                input = new PTInput(ipm.getKey().getPortName(), ipm.getValue(), oper, null, slidingUnifier.outputs.get(0), ipm.getKey().getValue(LogicalPlan.IS_CONNECTED_TO_DELAY_OPERATOR));
                sourceMapping.outputStreams.get(ipm.getValue().getSource()).slidingUnifiers.add(slidingUnifier);
              } else {
                input = new PTInput(ipm.getKey().getPortName(), ipm.getValue(), oper, null, sourceOut, ipm.getKey().getValue(LogicalPlan.IS_CONNECTED_TO_DELAY_OPERATOR));
              }
              oper.inputs.add(input);
            }
          }
        }
      } else {
        StreamMapping ug = sourceMapping.outputStreams.get(ipm.getValue().getSource());
        if (ug == null) {
          ug = new StreamMapping(ipm.getValue(), this);
          m.outputStreams.put(ipm.getValue().getSource(), ug);
        }
        LOG.debug("update upstream stream mapping for {} {}", sourceMapping.logicalOperator, ipm.getValue().getSource().getPortName());
        ug.setSources(sourceMapping.partitions);
      }
    }

  }

  public void deployChanges()
  {
    Set<PTContainer> newContainers = Sets.newHashSet();
    Set<PTContainer> releaseContainers = Sets.newHashSet();
    assignContainers(newContainers, releaseContainers);
    updatePartitionsInfoForPersistOperator(this.dag);

    // redeploy dependencies of the new operators excluding the new operators themselves
    Set<PTOperator> ndeps = getDependents(this.newOpers.keySet());
    this.undeployOpers.addAll(ndeps);
    this.undeployOpers.removeAll(this.newOpers.keySet());

    // include downstream dependencies of affected operators into redeploy
    Set<PTOperator> deployOperators = this.getDependents(this.deployOpers);
    // include new operators and their dependencies for deployment
    deployOperators.addAll(ndeps);

    //make sure all the new operators are included in deploy operator list
    this.deployOpers.addAll(this.newOpers.keySet());
    ctx.deploy(releaseContainers, this.undeployOpers, newContainers, deployOperators);
    this.newOpers.clear();
    this.deployOpers.clear();
    this.undeployOpers.clear();
  }

  private void assignContainers(Set<PTContainer> newContainers, Set<PTContainer> releaseContainers)
  {
    Set<PTOperator> mxnUnifiers = Sets.newHashSet();
    for (PTOperator o  : this.newOpers.keySet()) {
      mxnUnifiers.addAll(o.upstreamMerge.values());
    }
    Set<PTContainer> updatedContainers =  Sets.newHashSet();

    HashMap<PTOperator, PTContainer> operatorContainerMap = Maps.newHashMap();
    for (Map.Entry<PTOperator, Operator> operEntry : this.newOpers.entrySet()) {

      PTOperator oper = operEntry.getKey();
      Checkpoint checkpoint = getActivationCheckpoint(operEntry.getKey());
      initCheckpoint(oper, operEntry.getValue(), checkpoint);

      if (mxnUnifiers.contains(operEntry.getKey())) {
        // MxN unifiers are assigned with the downstream operator
        continue;
      }

      PTContainer newContainer = null;
      int memoryMB = 0;

      // handle container locality
      for (PTOperator inlineOper : oper.getGrouping(Locality.CONTAINER_LOCAL).getOperatorSet()) {
        if (inlineOper.container != null) {
          newContainer = inlineOper.container;
          break;
        }
        memoryMB += inlineOper.operatorMeta.getValue(OperatorContext.MEMORY_MB);
        memoryMB += inlineOper.getBufferServerMemory();
      }

      if (newContainer == null) {
        int vCores = getVCores(oper.getGrouping(Locality.CONTAINER_LOCAL).getOperatorSet());
        // attempt to find empty container with required size
        for (PTContainer c : this.containers) {
          if (c.operators.isEmpty() && c.getState() == PTContainer.State.ACTIVE && c.getAllocatedMemoryMB() == memoryMB && c.getAllocatedVCores() == vCores) {
            LOG.debug("Reusing existing container {} for {}", c, oper);
            c.setRequiredMemoryMB(0);
            c.setRequiredVCores(0);
            newContainer = c;
            break;
          }
        }
        if (newContainer == null) {
          // get new container
          LOG.debug("New container for: " + oper);
          newContainer = new PTContainer(this);
          newContainers.add(newContainer);
          containers.add(newContainer);
        }
        updatedContainers.add(newContainer);
      }
      setContainer(oper, newContainer);
    }
    // release containers that are no longer used and update operator to container map for applying anti-affinity
    for (PTContainer c : this.containers) {
      if (c.operators.isEmpty()) {
        LOG.debug("Container {} to be released", c);
        releaseContainers.add(c);
        containers.remove(c);
      } else {
        for (PTOperator oper : c.operators) {
          operatorContainerMap.put(oper, c);
        }
        c.getStrictAntiPrefs().clear();
        c.getPreferredAntiPrefs().clear();
      }
    }
    for (PTContainer c : updatedContainers) {
      updateContainerMemoryWithBufferServer(c);
      c.setRequiredVCores(getVCores(c.getOperators()));
    }

    AffinityRulesSet affinityRuleSet = dag.getAttributes().get(DAGContext.AFFINITY_RULES_SET);
    // Add anti-affinity restrictions in Containers
    if (affinityRuleSet != null && affinityRuleSet.getAffinityRules() != null) {
      setAntiAffinityForContainers(dag, affinityRuleSet.getAffinityRules(), operatorContainerMap);
    }
  }

  private void initCheckpoint(PTOperator oper, Operator oo, Checkpoint checkpoint)
  {
    try {
      LOG.debug("Writing activation checkpoint {} {} {}", checkpoint, oper, oo);
      long windowId = oper.isOperatorStateLess() ? Stateless.WINDOW_ID : checkpoint.windowId;
      StorageAgent agent = oper.operatorMeta.getValue(OperatorContext.STORAGE_AGENT);
      agent.save(oo, oper.id, windowId);
      if (agent instanceof AsyncStorageAgent) {
        ((AsyncStorageAgent)agent).flush(oper.id, windowId);
      }
    } catch (IOException e) {
      // inconsistent state, no recovery option, requires shutdown
      throw new IllegalStateException("Failed to write operator state after partition change " + oper, e);
    }
    oper.setRecoveryCheckpoint(checkpoint);
    if (!Checkpoint.INITIAL_CHECKPOINT.equals(checkpoint)) {
      oper.checkpoints.add(checkpoint);
    }
  }

  public Operator loadOperator(PTOperator oper)
  {
    try {
      LOG.debug("Loading state for {}", oper);
      return (Operator)oper.operatorMeta.getValue(OperatorContext.STORAGE_AGENT).load(oper.id, oper.isOperatorStateLess() ? Stateless.WINDOW_ID : oper.recoveryCheckpoint.windowId);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read partition state for " + oper, e);
    }
  }

  /**
   * Initialize the activation checkpoint for the given operator.
   * Recursively traverses inputs until existing checkpoint or root operator is found.
   * NoOp when already initialized.
   * @param oper
   */
  private Checkpoint getActivationCheckpoint(PTOperator oper)
  {
    if (oper.recoveryCheckpoint == null && oper.checkpoints.isEmpty()) {
      Checkpoint activationCheckpoint = Checkpoint.INITIAL_CHECKPOINT;
      for (PTInput input : oper.inputs) {
        PTOperator sourceOper = input.source.source;
        Checkpoint checkpoint = sourceOper.recoveryCheckpoint;
        if (sourceOper.checkpoints.isEmpty()) {
          checkpoint = getActivationCheckpoint(sourceOper);
        }
        activationCheckpoint = Checkpoint.max(activationCheckpoint, checkpoint);
      }
      return activationCheckpoint;
    }
    return oper.recoveryCheckpoint;
  }

  /**
   * Remove a partition that was reported as terminated by the execution layer.
   * Recursively removes all downstream operators with no remaining input.
   * @param p
   */
  public void removeTerminatedPartition(PTOperator p)
  {
    // keep track of downstream operators for cascading remove
    Set<PTOperator> downstreamOpers = new HashSet<>(p.outputs.size());
    for (PTOutput out : p.outputs) {
      for (PTInput sinkIn : out.sinks) {
        downstreamOpers.add(sinkIn.target);
      }
    }
    PMapping currentMapping = this.logicalToPTOperator.get(p.operatorMeta);
    if (currentMapping != null) {
      List<PTOperator> copyPartitions = Lists.newArrayList(currentMapping.partitions);
      copyPartitions.remove(p);
      removePartition(p, currentMapping);
      currentMapping.partitions = copyPartitions;
    } else {
      // remove the operator
      removePTOperator(p);
    }
    // remove orphaned downstream operators
    for (PTOperator dop : downstreamOpers) {
      if (dop.inputs.isEmpty()) {
        removeTerminatedPartition(dop);
      }
    }
    deployChanges();
  }

  /**
   * Remove the given partition with any associated parallel partitions and
   * per-partition outputStreams.
   *
   * @param oper
   * @return
   */
  private void removePartition(PTOperator oper, PMapping operatorMapping)
  {
    // remove any parallel partition
    for (PTOutput out : oper.outputs) {
      // copy list as it is modified by recursive remove
      for (PTInput in : Lists.newArrayList(out.sinks)) {
        for (LogicalPlan.InputPortMeta im : in.logicalStream.getSinks()) {
          PMapping m = this.logicalToPTOperator.get(im.getOperatorMeta());
          if (m.parallelPartitions == operatorMapping.parallelPartitions) {
            // associated operator parallel partitioned
            removePartition(in.target, operatorMapping);
            m.partitions.remove(in.target);
          }
        }
      }
    }
    // remove the operator
    removePTOperator(oper);
  }

  private PTOperator addPTOperator(PMapping nodeDecl, Partition<? extends Operator> partition, Checkpoint checkpoint)
  {
    PTOperator oper = newOperator(nodeDecl.logicalOperator, nodeDecl.logicalOperator.getName());
    oper.recoveryCheckpoint = checkpoint;

    // output port objects
    for (Map.Entry<LogicalPlan.OutputPortMeta, StreamMeta> outputEntry : nodeDecl.logicalOperator.getOutputStreams().entrySet()) {
      setupOutput(nodeDecl, oper, outputEntry);
    }

    String host = null;
    if (partition != null) {
      oper.setPartitionKeys(partition.getPartitionKeys());
      host = partition.getAttributes().get(OperatorContext.LOCALITY_HOST);
    }
    if (host == null) {
      host = nodeDecl.logicalOperator.getValue(OperatorContext.LOCALITY_HOST);
    }

    nodeDecl.addPartition(oper);
    this.newOpers.put(oper, partition != null ? partition.getPartitionedInstance() : nodeDecl.logicalOperator.getOperator());

    //
    // update locality
    //
    setLocalityGrouping(nodeDecl, oper, inlinePrefs, Locality.CONTAINER_LOCAL, host);
    setLocalityGrouping(nodeDecl, oper, localityPrefs, Locality.NODE_LOCAL, host);

    return oper;
  }

  /**
   * Create output port mapping for given operator and port.
   * Occurs when adding new partition or new logical stream.
   * Does nothing if source was already setup (on add sink to existing stream).
   * @param mapping
   * @param oper
   * @param outputEntry
   */
  private void setupOutput(PMapping mapping, PTOperator oper, Map.Entry<LogicalPlan.OutputPortMeta, StreamMeta> outputEntry)
  {
    for (PTOutput out : oper.outputs) {
      if (out.logicalStream == outputEntry.getValue()) {
        // already processed
        return;
      }
    }

    PTOutput out = new PTOutput(outputEntry.getKey().getPortName(), outputEntry.getValue(), oper);
    oper.outputs.add(out);
  }

  PTOperator newOperator(OperatorMeta om, String name)
  {
    PTOperator oper = new PTOperator(this, idSequence.incrementAndGet(), name, om);
    allOperators.put(oper.id, oper);
    oper.inputs = new ArrayList<>();
    oper.outputs = new ArrayList<>();

    this.ctx.recordEventAsync(new StramEvent.CreateOperatorEvent(oper.getName(), oper.getId()));

    return oper;
  }

  private void setLocalityGrouping(PMapping pnodes, PTOperator newOperator, LocalityPrefs localityPrefs, Locality ltype,String host)
  {
    HostOperatorSet grpObj = newOperator.getGrouping(ltype);
    if (host != null) {
      grpObj.setHost(host);
    }
    Set<PTOperator> s = grpObj.getOperatorSet();
    s.add(newOperator);
    LocalityPref loc = localityPrefs.prefs.get(pnodes);
    if (loc != null) {
      for (PMapping localPM : loc.operators) {
        if (pnodes.parallelPartitions == localPM.parallelPartitions) {
          if (localPM.partitions.size() >= pnodes.partitions.size()) {
            // apply locality setting per partition
            s.addAll(localPM.partitions.get(pnodes.partitions.size() - 1).getGrouping(ltype).getOperatorSet());
          }
        } else {
          for (PTOperator otherNode : localPM.partitions) {
            s.addAll(otherNode.getGrouping(ltype).getOperatorSet());
          }
        }
      }
      for (PTOperator localOper : s) {
        if (grpObj.getHost() == null) {
          grpObj.setHost(localOper.groupings.get(ltype).getHost());
        }
        localOper.groupings.put(ltype, grpObj);
      }
    }
  }

  private List<InputPort<?>> getInputPortList(LogicalPlan.OperatorMeta operatorMeta)
  {
    List<InputPort<?>> inputPortList = Lists.newArrayList();

    for (InputPortMeta inputPortMeta: operatorMeta.getInputStreams().keySet()) {
      inputPortList.add(inputPortMeta.getPort());
    }

    return inputPortList;
  }

  void removePTOperator(PTOperator oper)
  {
    LOG.debug("Removing operator {}", oper);

    // per partition merge operators
    if (!oper.upstreamMerge.isEmpty()) {
      for (PTOperator unifier : oper.upstreamMerge.values()) {
        removePTOperator(unifier);
      }
    }

    // remove inputs from downstream operators
    for (PTOutput out : oper.outputs) {
      for (PTInput sinkIn : out.sinks) {
        if (sinkIn.source.source == oper) {
          ArrayList<PTInput> cowInputs = Lists.newArrayList(sinkIn.target.inputs);
          cowInputs.remove(sinkIn);
          sinkIn.target.inputs = cowInputs;
        }
      }
    }
    // remove from upstream operators
    for (PTInput in : oper.inputs) {
      in.source.sinks.remove(in);
    }

    for (HostOperatorSet s : oper.groupings.values()) {
      s.getOperatorSet().remove(oper);
    }

    // remove checkpoint states
    try {
      synchronized (oper.checkpoints) {
        for (Checkpoint checkpoint : oper.checkpoints) {
          oper.operatorMeta.getValue(OperatorContext.STORAGE_AGENT).delete(oper.id, checkpoint.windowId);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to remove state for " + oper, e);
    }

    List<PTOperator> cowList = Lists.newArrayList(oper.container.operators);
    cowList.remove(oper);
    oper.container.operators = cowList;
    this.deployOpers.remove(oper);
    this.undeployOpers.add(oper);
    this.allOperators.remove(oper.id);
    this.ctx.recordEventAsync(new StramEvent.RemoveOperatorEvent(oper.getName(), oper.getId()));
  }

  public PlanContext getContext()
  {
    return ctx;
  }

  public LogicalPlan getLogicalPlan()
  {
    return this.dag;
  }

  public List<PTContainer> getContainers()
  {
    return this.containers;
  }

  public Map<Integer, PTOperator> getAllOperators()
  {
    return this.allOperators;
  }

  /**
   * Get the partitions for the logical operator.
   * Partitions represent instances of the operator and do not include any unifiers.
   * @param logicalOperator
   * @return
   */
  public List<PTOperator> getOperators(OperatorMeta logicalOperator)
  {
    return this.logicalToPTOperator.get(logicalOperator).partitions;
  }

  public Collection<PTOperator> getAllOperators(OperatorMeta logicalOperator)
  {
    return this.logicalToPTOperator.get(logicalOperator).getAllOperators();
  }

  public List<PTOperator> getLeafOperators()
  {
    List<PTOperator> operators = new ArrayList<>();
    for (OperatorMeta opMeta : dag.getLeafOperators()) {
      operators.addAll(getAllOperators(opMeta));
    }
    return operators;
  }

  public boolean hasMapping(OperatorMeta om)
  {
    return this.logicalToPTOperator.containsKey(om);
  }

  // used for testing only
  @VisibleForTesting
  public List<PTOperator> getMergeOperators(OperatorMeta logicalOperator)
  {
    List<PTOperator> opers = Lists.newArrayList();
    for (StreamMapping ug : this.logicalToPTOperator.get(logicalOperator).outputStreams.values()) {
      ug.addTo(opers);
    }
    return opers;
  }

  protected List<OperatorMeta> getRootOperators()
  {
    return dag.getRootOperators();
  }

  private void getDeps(PTOperator operator, Set<PTOperator> visited)
  {
    visited.add(operator);
    for (PTInput in : operator.inputs) {
      if (in.source.isDownStreamInline()) {
        PTOperator sourceOperator = in.source.source;
        if (!visited.contains(sourceOperator)) {
          getDeps(sourceOperator, visited);
        }
      }
    }
    // downstream traversal
    for (PTOutput out: operator.outputs) {
      for (PTInput sink : out.sinks) {
        PTOperator sinkOperator = sink.target;
        if (!visited.contains(sinkOperator)) {
          getDeps(sinkOperator, visited);
        }
      }
    }
  }

  /**
   * Get all operator instances that depend on the specified operator instance(s).
   * Dependencies are all downstream and upstream inline operators.
   * @param operators
   * @return
   */
  public Set<PTOperator> getDependents(Collection<PTOperator> operators)
  {
    Set<PTOperator> visited = new LinkedHashSet<>();
    if (operators != null) {
      for (PTOperator operator: operators) {
        getDeps(operator, visited);
      }
    }
    visited.addAll(getDependentPersistOperators(operators));
    return visited;
  }

  private Set<PTOperator> getDependentPersistOperators(Collection<PTOperator> operators)
  {
    Set<PTOperator> persistOperators = new LinkedHashSet<>();
    if (operators != null) {
      for (PTOperator operator : operators) {
        for (PTInput in : operator.inputs) {
          if (in.logicalStream.getPersistOperator() != null) {
            for (InputPortMeta inputPort : in.logicalStream.getSinksToPersist()) {
              if (inputPort.getOperatorMeta().equals(operator.operatorMeta)) {
                // Redeploy the stream wide persist operator only if the current sink is being persisted
                persistOperators.addAll(getOperators(in.logicalStream.getPersistOperator()));
                break;
              }
            }
          }
          for (Map.Entry<InputPortMeta, OperatorMeta> entry : in.logicalStream.sinkSpecificPersistOperatorMap.entrySet()) {
            // Redeploy sink specific persist operators
            persistOperators.addAll(getOperators(entry.getValue()));
          }
        }
      }
    }
    return persistOperators;
  }

  /**
   * Add logical operator to the plan. Assumes that upstream operators have been added before.
   * @param om
   */
  public final void addLogicalOperator(OperatorMeta om)
  {
    PMapping pnodes = new PMapping(om);
    String host = pnodes.logicalOperator.getValue(OperatorContext.LOCALITY_HOST);
    localityPrefs.add(pnodes, host);

    PMapping upstreamPartitioned = null;

    for (Map.Entry<LogicalPlan.InputPortMeta, StreamMeta> e : om.getInputStreams().entrySet()) {
      if (e.getValue().getSource().getOperatorMeta().getOperator() instanceof Operator.DelayOperator) {
        continue;
      }
      PMapping m = logicalToPTOperator.get(e.getValue().getSource().getOperatorMeta());
      if (e.getKey().getValue(PortContext.PARTITION_PARALLEL).equals(true)) {
        // operator partitioned with upstream
        if (upstreamPartitioned != null) {
          // need to have common root
          if (!upstreamPartitioned.parallelPartitions.contains(m.logicalOperator) && upstreamPartitioned != m) {
            String msg = String.format("operator cannot extend multiple partitions (%s and %s)", upstreamPartitioned.logicalOperator, m.logicalOperator);
            throw new AssertionError(msg);
          }
        }
        m.parallelPartitions.add(pnodes.logicalOperator);
        pnodes.parallelPartitions = m.parallelPartitions;
        upstreamPartitioned = m;
      }

      if (Locality.CONTAINER_LOCAL == e.getValue().getLocality() || Locality.THREAD_LOCAL == e.getValue().getLocality()) {
        inlinePrefs.setLocal(m, pnodes);
      } else if (Locality.NODE_LOCAL == e.getValue().getLocality()) {
        localityPrefs.setLocal(m, pnodes);
      }
    }

    //
    // create operator instances
    //
    this.logicalToPTOperator.put(om, pnodes);
    if (upstreamPartitioned != null) {
      // parallel partition
      //LOG.debug("Operator {} should be partitioned to {} partitions", pnodes.logicalOperator.getName(), upstreamPartitioned.partitions.size());
      initPartitioning(pnodes, upstreamPartitioned.partitions.size());
    } else {
      initPartitioning(pnodes, 0);
    }
    updateStreamMappings(pnodes);
  }

  /**
   * Remove physical representation of given stream. Operators that are affected
   * in the execution layer will be added to the set. This method does not
   * automatically remove operators from the plan.
   *
   * @param sm
   */
  public void removeLogicalStream(StreamMeta sm)
  {
    // remove incoming connections for logical stream
    for (InputPortMeta ipm : sm.getSinks()) {
      OperatorMeta om = ipm.getOperatorMeta();
      PMapping m = this.logicalToPTOperator.get(om);
      if (m == null) {
        throw new AssertionError("Unknown operator " + om);
      }
      for (PTOperator oper : m.partitions) {
        List<PTInput> inputsCopy = Lists.newArrayList(oper.inputs);
        for (PTInput input : oper.inputs) {
          if (input.logicalStream == sm) {
            input.source.sinks.remove(input);
            inputsCopy.remove(input);
            undeployOpers.add(oper);
            deployOpers.add(oper);
          }
        }
        oper.inputs = inputsCopy;
      }
    }
    // remove outgoing connections for logical stream
    PMapping m = this.logicalToPTOperator.get(sm.getSource().getOperatorMeta());
    for (PTOperator oper : m.partitions) {
      List<PTOutput> outputsCopy = Lists.newArrayList(oper.outputs);
      for (PTOutput out : oper.outputs) {
        if (out.logicalStream == sm) {
          for (PTInput input : out.sinks) {
            PTOperator downstreamOper = input.source.source;
            downstreamOper.inputs.remove(input);
            Set<PTOperator> deps = this.getDependents(Collections.singletonList(downstreamOper));
            undeployOpers.addAll(deps);
            deployOpers.addAll(deps);
          }
          outputsCopy.remove(out);
          undeployOpers.add(oper);
          deployOpers.add(oper);
        }
      }
      oper.outputs = outputsCopy;
    }
  }

  /**
   * Connect operators through stream. Currently new stream will not affect locality.
   * @param ipm Meta information about the input port
   */
  public void connectInput(InputPortMeta ipm)
  {
    for (Map.Entry<LogicalPlan.InputPortMeta, StreamMeta> inputEntry : ipm.getOperatorMeta().getInputStreams().entrySet()) {
      if (inputEntry.getKey() == ipm) {
        // initialize outputs for existing operators
        for (Map.Entry<LogicalPlan.OutputPortMeta, StreamMeta> outputEntry : inputEntry.getValue().getSource().getOperatorMeta().getOutputStreams().entrySet()) {
          PMapping sourceOpers = this.logicalToPTOperator.get(outputEntry.getKey().getOperatorMeta());
          for (PTOperator oper : sourceOpers.partitions) {
            setupOutput(sourceOpers, oper, outputEntry); // idempotent
            undeployOpers.add(oper);
            deployOpers.add(oper);
          }
        }
        PMapping m = this.logicalToPTOperator.get(ipm.getOperatorMeta());
        updateStreamMappings(m);
        for (PTOperator oper : m.partitions) {
          undeployOpers.add(oper);
          deployOpers.add(oper);
        }
      }
    }
  }

  /**
   * Remove all physical operators for the given logical operator.
   * All connected streams must have been previously removed.
   * @param om
   */
  public void removeLogicalOperator(OperatorMeta om)
  {
    PMapping opers = this.logicalToPTOperator.get(om);
    if (opers == null) {
      throw new AssertionError("Operator not in physical plan: " + om.getName());
    }

    for (PTOperator oper : opers.partitions) {
      removePartition(oper, opers);
    }

    for (StreamMapping ug : opers.outputStreams.values()) {
      for (PTOperator oper : ug.cascadingUnifiers) {
        removePTOperator(oper);
      }
      if (ug.finalUnifier != null) {
        removePTOperator(ug.finalUnifier);
      }
    }

    LinkedHashMap<OperatorMeta, PMapping> copyMap = Maps.newLinkedHashMap(this.logicalToPTOperator);
    copyMap.remove(om);
    this.logicalToPTOperator = copyMap;
  }

  public void setAvailableResources(int memoryMB)
  {
    this.availableMemoryMB = memoryMB;
  }

  public void onStatusUpdate(PTOperator oper)
  {
    for (StatsListenerWithContext l : oper.statsListeners) {
      final StatsListener.Response rsp = l.processStats(oper.stats, statsListenerContext);
      if (rsp != null) {
        //LOG.debug("Response to processStats = {}", rsp.repartitionRequired);
        oper.loadIndicator = rsp.loadIndicator;
        if (rsp.repartitionRequired) {
          final OperatorMeta om = oper.getOperatorMeta();
          // concurrent heartbeat processing
          if (this.pendingRepartition.putIfAbsent(om, om) != null) {
            LOG.debug("Skipping repartitioning for {} load {}", oper, oper.loadIndicator);
          } else {
            LOG.debug("Scheduling repartitioning for {} load {}", oper, oper.loadIndicator);
            // hand over to monitor thread
            Runnable r = new Runnable()
            {
              @Override
              public void run()
              {
                redoPartitions(logicalToPTOperator.get(om), rsp.repartitionNote);
                pendingRepartition.remove(om);
              }
            };
            ctx.dispatch(r);
          }
        }
        if (rsp.operatorRequests != null) {
          for (OperatorRequest cmd : rsp.operatorRequests) {
            StramToNodeRequest request = new StramToNodeRequest();
            request.operatorId = oper.getId();
            request.requestType = StramToNodeRequest.RequestType.CUSTOM;
            request.cmd = cmd;
            ctx.addOperatorRequest(oper, request);
          }
        }
        // for backward compatibility
        if (rsp.operatorCommands != null) {
          for (@SuppressWarnings("deprecation") com.datatorrent.api.StatsListener.OperatorCommand cmd : rsp.operatorCommands) {
            StramToNodeRequest request = new StramToNodeRequest();
            request.operatorId = oper.getId();
            request.requestType = StramToNodeRequest.RequestType.CUSTOM;
            OperatorCommandConverter converter = new OperatorCommandConverter();
            converter.cmd = cmd;
            request.cmd = converter;
            ctx.addOperatorRequest(oper, request);
          }
        }
      }
    }
  }

  /**
   * Read available checkpoints from storage agent for all operators.
   * @param startTime
   * @param currentTime
   * @throws IOException
   */
  public void syncCheckpoints(long startTime, long currentTime) throws IOException
  {
    for (PTOperator oper : getAllOperators().values()) {
      StorageAgent sa = oper.operatorMeta.getValue(OperatorContext.STORAGE_AGENT);
      long[] windowIds = sa.getWindowIds(oper.getId());
      Arrays.sort(windowIds);
      oper.checkpoints.clear();
      for (long wid : windowIds) {
        if (wid != Stateless.WINDOW_ID) {
          oper.addCheckpoint(wid, startTime);
        }
      }
    }
  }

  public Integer getStreamCodecIdentifier(StreamCodec<?> streamCodecInfo)
  {
    Integer id;
    synchronized (streamCodecIdentifiers) {
      id = streamCodecIdentifiers.get(streamCodecInfo);
      if (id == null) {
        id = strCodecIdSequence.incrementAndGet();
        streamCodecIdentifiers.put(streamCodecInfo, id);
      }
    }
    return id;
  }

  @VisibleForTesting
  public Map<StreamCodec<?>, Integer> getStreamCodecIdentifiers()
  {
    return Collections.unmodifiableMap(streamCodecIdentifiers);
  }

  /**
   * This is for backward compatibility
   */
  public static class OperatorCommandConverter implements OperatorRequest,Serializable
  {
    private static final long serialVersionUID = 1L;
    @SuppressWarnings("deprecation")
    public com.datatorrent.api.StatsListener.OperatorCommand cmd;
    @SuppressWarnings("deprecation")
    @Override
    public StatsListener.OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
    {
      cmd.execute(operator,operatorId,windowId);
      return null;
    }
  }

  /**
   * An implementation for {@link StatsListenerContext} to access the DAG. Currently only method to extract
   * operator name from operator instance id is provided. In future additional methods could be added.
   */
  private class StatsListenerContextImpl implements StatsListenerContext, Serializable
  {
    @Override
    public String getOperatorName(int id)
    {
      PTOperator operator = getAllOperators().get(id);
      if (operator != null) {
        return operator.getName();
      }
      return null;
    }
  }

}

/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan.physical;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitionable.Partition;
import com.datatorrent.api.Partitionable.PartitionKeys;

import com.datatorrent.stram.Journal.RecoverableOperation;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
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

  /**
   * Stats listener for throughput based partitioning.
   * Used when thresholds are configured on operator through attributes.
   */
  public static class PartitionLoadWatch implements StatsListener, java.io.Serializable
  {
    private static final long serialVersionUID = 201312231633L;
    public long evalIntervalMillis = 30*1000;
    private final long tpsMin;
    private final long tpsMax;
    private final PMapping operMapping;
    private long lastEvalMillis;
    private long lastTps = 0;

    private PartitionLoadWatch(PMapping operMapping, long min, long max) {
      this.tpsMin = min;
      this.tpsMax = max;
      this.operMapping = operMapping;
    }

    protected int getLoadIndicator(int operatorId, long tps) {
      if ((tps < tpsMin && lastTps != 0) || tps > tpsMax) {
        lastTps = tps;
        return (tps < tpsMin) ? -1 : 1;
      }
      lastTps = tps;
      return 0;
    }

    @Override
    public Response processStats(BatchedOperatorStats status)
    {
      Response rsp = new Response();
      long tps = status.getTuplesProcessedPSMA();
      if (operMapping.logicalOperator.getInputStreams().isEmpty()) {
        tps = status.getTuplesProcessedPSMA();
      }
      rsp.loadIndicator = getLoadIndicator(status.getOperatorId(), tps);
      if (rsp.loadIndicator != 0) {
        if (lastEvalMillis < (System.currentTimeMillis() - evalIntervalMillis)) {
          lastEvalMillis = System.currentTimeMillis();
          LOG.debug("Requesting repartitioning for {}/{} {} {}", new Object[] {operMapping.logicalOperator, status.getOperatorId(), rsp.loadIndicator, tps});
          rsp.repartitionRequired = true;
        }
      }
      return rsp;
    }

  }

  private final AtomicInteger idSequence = new AtomicInteger();
  final AtomicInteger containerSeq = new AtomicInteger();
  private LinkedHashMap<OperatorMeta, PMapping> logicalToPTOperator = new LinkedHashMap<OperatorMeta, PMapping>();
  private final List<PTContainer> containers = new CopyOnWriteArrayList<PTContainer>();
  private final LogicalPlan dag;
  private transient final PlanContext ctx;
  private int maxContainers = 1;
  private final LocalityPrefs localityPrefs = new LocalityPrefs();
  private final LocalityPrefs inlinePrefs = new LocalityPrefs();

  final Set<PTOperator> deployOpers = Sets.newHashSet();
  final Map<PTOperator, Operator> newOpers = Maps.newHashMap();
  final Set<PTOperator> undeployOpers = Sets.newHashSet();
  final ConcurrentMap<Integer, PTOperator> allOperators = Maps.newConcurrentMap();
  private final ConcurrentMap<OperatorMeta, OperatorMeta> pendingRepartition = Maps.newConcurrentMap();

  private PTContainer getContainer(int index) {
    if (index >= containers.size()) {
      if (index >= maxContainers) {
        index = maxContainers - 1;
      }
      for (int i=containers.size(); i<index+1; i++) {
        containers.add(i, new PTContainer(this));
      }
    }
    return containers.get(index);
  }

  /**
   * Interface to execution context that can be mocked for plan testing.
   */
  public interface PlanContext {

    /**
     * Dynamic partitioning requires access to operator state for split or merge.
     * @return
     */
    public StorageAgent getStorageAgent();

    /**
     * Record an event in the event log
     *
     * @param ev The event
     *
     */
    public void recordEventAsync(StramEvent ev);

    /**
     * Request deployment change as sequence of undeploy, container start and deploy groups with dependency.
     * Called on initial plan and on dynamic changes during execution.
     */
    public void deploy(Set<PTContainer> releaseContainers, Collection<PTOperator> undeploy, Set<PTContainer> startContainers, Collection<PTOperator> deploy);

    /**
     * Trigger event to perform plan modification.
     * @param r
     */
    public void dispatch(Runnable r);

    /**
     * Write the recoverable operation to the log.
     * @param op
     */
    public void writeJournal(RecoverableOperation op);

  }

  /**
   * The logical operator with physical plan info tagged on.
   */
  public static class PMapping implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312112033L;

    final private OperatorMeta logicalOperator;
    private List<PTOperator> partitions = new LinkedList<PTOperator>();
    private final Map<LogicalPlan.OutputPortMeta, StreamMapping> outputStreams = Maps.newHashMap();
    private List<StatsListener> statsHandlers;

    /**
     * Operators that form a parallel partition
     */
    private Set<OperatorMeta> parallelPartitions = Sets.newHashSet();

    private PMapping(OperatorMeta om) {
      this.logicalOperator = om;
    }

    private void addPartition(PTOperator p) {
      partitions.add(p);
      p.statsListeners = this.statsHandlers;
    }

    private Collection<PTOperator> getAllOperators() {
      if (partitions.size() == 1) {
        return Collections.singletonList(partitions.get(0));
      }
      Collection<PTOperator> c = new ArrayList<PTOperator>(partitions.size() + 1);
      c.addAll(partitions);
      for (StreamMapping ug : outputStreams.values()) {
        ug.addTo(c);
      }
      return c;
    }

    @Override
    public String toString() {
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

    void add(PMapping m, String group) {
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

    @SuppressWarnings("null") /* for lp2.operators.add(m1); line below - netbeans is not very smart */
    void setLocal(PMapping m1, PMapping m2) {
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
  public PhysicalPlan(LogicalPlan dag, PlanContext ctx) {

    this.dag = dag;
    this.ctx = ctx;
    this.maxContainers = Math.max(dag.getMaxContainerCount(), 1);
    LOG.debug("Max containers: {}", this.maxContainers);

    Stack<OperatorMeta> pendingNodes = new Stack<OperatorMeta>();

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

      for (StreamMeta s : n.getInputStreams().values()) {
        if (s.getSource() != null && !this.logicalToPTOperator.containsKey(s.getSource().getOperatorWrapper())) {
          pendingNodes.push(n);
          pendingNodes.push(s.getSource().getOperatorWrapper());
          upstreamDeployed = false;
          break;
        }
      }

      if (upstreamDeployed) {
        addLogicalOperator(n);
      }
    }

    // assign operators to containers
    int groupCount = 0;
    Set<PTOperator> deployOperators = Sets.newHashSet();
    for (Map.Entry<OperatorMeta, PMapping> e : logicalToPTOperator.entrySet()) {
      for (PTOperator oper : e.getValue().getAllOperators()) {
        if (oper.container == null) {
          PTContainer container = getContainer((groupCount++) % maxContainers);
          Set<PTOperator> inlineSet = oper.getGrouping(Locality.CONTAINER_LOCAL).getOperatorSet();
          if (!inlineSet.isEmpty()) {
            // process inline operators
            for (PTOperator inlineOper : inlineSet) {
              setContainer(inlineOper, container);
            }
          } else {
            setContainer(oper, container);
          }
          deployOperators.addAll(container.operators);
        }
      }
    }

    for (Map.Entry<PTOperator, Operator> operEntry : this.newOpers.entrySet()) {
      initCheckpoint(operEntry.getKey(), operEntry.getValue(), OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID);
    }
    // request initial deployment
    ctx.deploy(Collections.<PTContainer>emptySet(), Collections.<PTOperator>emptySet(), Sets.newHashSet(containers), deployOperators);
    this.newOpers.clear();
    this.deployOpers.clear();
    this.undeployOpers.clear();

  }

  private void setContainer(PTOperator pOperator, PTContainer container) {
    LOG.debug("Setting container {} for {}", container, pOperator);
    assert (pOperator.container == null) : "Container already assigned for " + pOperator;
    pOperator.container = container;
    container.operators.add(pOperator);
    if (!pOperator.upstreamMerge.isEmpty()) {
      for (Map.Entry<InputPortMeta, PTOperator> mEntry : pOperator.upstreamMerge.entrySet()) {
        assert (mEntry.getValue().container == null) : "Container already assigned for " + mEntry.getValue();
        mEntry.getValue().container = container;
        container.operators.add(mEntry.getValue());
      }
    }
  }

  private void initPartitioning(PMapping m)
  {
    int partitionCnt = m.logicalOperator.getValue(OperatorContext.INITIAL_PARTITION_COUNT);
    if (partitionCnt == 0) {
      LOG.warn("Ignoring invalid value 0 for {} {}", m.logicalOperator, OperatorContext.INITIAL_PARTITION_COUNT);
      partitionCnt = 1;
    }

    Operator operator = m.logicalOperator.getOperator();
    Collection<Partition<Operator>> partitions = new ArrayList<Partition<Operator>>(1);
    if (operator instanceof Partitionable) {
      @SuppressWarnings({ "unchecked", "rawtypes" })
      Partitionable<Operator> partitionable = (Partitionable)operator;
      // operator to provide initial partitioning
      partitions.add(new DefaultPartition<Operator>(operator));
      partitions = partitionable.definePartitions(partitions, partitionCnt - 1);
    }
    else {
      partitions = new OperatorPartitions.DefaultPartitioner().defineInitialPartitions(m.logicalOperator, partitionCnt);
    }

    if (partitions == null || partitions.isEmpty()) {
      throw new AssertionError("PartitionableOperator must return at least one partition: " + m.logicalOperator);
    }

    int minTps = m.logicalOperator.getValue(OperatorContext.PARTITION_TPS_MIN);
    int maxTps = m.logicalOperator.getValue(OperatorContext.PARTITION_TPS_MAX);
    if (maxTps > minTps) {
      if (m.statsHandlers == null) {
        m.statsHandlers = new ArrayList<StatsListener>(1);
      }
      m.statsHandlers.add(new PartitionLoadWatch(m, minTps, maxTps));
    }

    Collection<StatsListener> statsListeners = m.logicalOperator.getValue(OperatorContext.STATS_LISTENERS);
    if (statsListeners != null && !statsListeners.isEmpty()) {
      if (m.statsHandlers == null) {
        m.statsHandlers = new ArrayList<StatsListener>(statsListeners.size());
      }
      m.statsHandlers.addAll(statsListeners);
    }

    if (m.logicalOperator.getOperator() instanceof StatsListener) {
      if (m.statsHandlers == null) {
        m.statsHandlers = new ArrayList<StatsListener>(1);
      }
      m.statsHandlers.add((StatsListener)m.logicalOperator.getOperator());
    }


    /*
    if (statsListeners != null) {
      if (m.statsHandlers == null) {
        m.statsHandlers = new ArrayList<StatsListener>(1);
      }
      final StatsListener sh;
      if (PartitionLoadWatch.class.isAssignableFrom(statsListeners)) {
        try {
          sh = statsListeners.getConstructor(m.getClass()).newInstance(m);
        }
        catch (Exception e) {
          throw new RuntimeException("Failed to instantiate stats listener.", e);
        }
      }
      else {
        sh = StramUtils.newInstance(statsListeners);
      }
      m.statsHandlers.add(sh);
    }
    */

    // create operator instance per partition
    Map<Integer, Partition<Operator>> operatorIdToPartition = Maps.newHashMapWithExpectedSize(partitions.size());
    for (Partition<Operator> partition : partitions) {
      PTOperator p = addPTOperator(m, partition, OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID);
      operatorIdToPartition.put(p.getId(), partition);
    }
    updateStreamMappings(m);

    if (operator instanceof Partitionable.PartitionAware) {
      @SuppressWarnings({ "unchecked", "rawtypes" })
      Partitionable.PartitionAware<Operator> partitionable = (Partitionable.PartitionAware)operator;
      partitionable.partitioned(operatorIdToPartition);
    }

  }

  private void redoPartitions(PMapping currentMapping)
  {
    // collect current partitions with committed operator state
    // those will be needed by the partitioner for split/merge
    List<PTOperator> operators = currentMapping.partitions;
    List<DefaultPartition<Operator>> currentPartitions = new ArrayList<DefaultPartition<Operator>>(operators.size());
    Map<Partition<?>, PTOperator> currentPartitionMap = Maps.newHashMapWithExpectedSize(operators.size());
    Map<Integer, Partition<Operator>> operatorIdToPartition = Maps.newHashMapWithExpectedSize(operators.size());

    final Collection<Partition<Operator>> newPartitions;
    long minCheckpoint = OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID;

    for (PTOperator pOperator : operators) {
      Map<InputPort<?>, PartitionKeys> pks = pOperator.getPartitionKeys();
      if (pks == null) {
        throw new AssertionError("Null partition: " + pOperator);
      }

      // if partitions checkpoint at different windows, processing for new or modified
      // partitions will start from earliest checkpoint found (at least once semantics)
      if (minCheckpoint < 0) {
        minCheckpoint = pOperator.recoveryCheckpoint;
      } else {
        minCheckpoint = Math.min(minCheckpoint, pOperator.recoveryCheckpoint);
      }

      Operator partitionedOperator = loadOperator(pOperator);
      DefaultPartition<Operator> partition = new DefaultPartition<Operator>(partitionedOperator, pks, pOperator.loadIndicator, pOperator.stats);
      currentPartitions.add(partition);
      currentPartitionMap.put(partition, pOperator);
      operatorIdToPartition.put(pOperator.getId(), partition);
    }

    for (Map.Entry<Partition<?>, PTOperator> e : currentPartitionMap.entrySet()) {
      LOG.debug("partition load: {} {} {}", new Object[] {e.getValue(), e.getKey().getPartitionKeys(), e.getKey().getLoad()});
    }
    if (currentMapping.logicalOperator.getOperator() instanceof Partitionable) {
      // would like to know here how much more capacity we have here so that definePartitions can act accordingly.
      final int incrementalCapacity = 0;
      @SuppressWarnings({ "unchecked", "rawtypes" })
      Partitionable<Operator> partitionable = (Partitionable)currentMapping.logicalOperator.getOperator();
      newPartitions = partitionable.definePartitions(new ArrayList<Partition<Operator>>(currentPartitions), incrementalCapacity);
    } else {
      if (!currentMapping.logicalOperator.getInputStreams().isEmpty()) {
        newPartitions = new OperatorPartitions.DefaultPartitioner().repartition(currentPartitions);
      } else {
        newPartitions = OperatorPartitions.DefaultPartitioner.repartitionInputOperator(currentPartitions);
      }
    }

    if (newPartitions.isEmpty()) {
      LOG.warn("Empty partition list after repartition: {}", currentMapping.logicalOperator);
      return;
    }

    List<Partition<Operator>> addedPartitions = new ArrayList<Partition<Operator>>();
    // determine modifications of partition set, identify affected operator instance(s)
    for (Partition<Operator> newPartition : newPartitions) {
      PTOperator op = currentPartitionMap.remove(newPartition);
      if (op == null) {
        addedPartitions.add(newPartition);
      } else {
        // check whether mapping was changed
        for (DefaultPartition<Operator> pi : currentPartitions) {
          if (pi == newPartition && pi.isModified()) {
            // existing partition changed (operator or partition keys)
            // remove/add to update subscribers and state
            currentPartitionMap.put(newPartition, op);
            addedPartitions.add(newPartition);
          }
        }
      }
    }

    // remaining entries represent deprecated partitions
    this.undeployOpers.addAll(currentPartitionMap.values());
    // downstream dependencies require redeploy, resolve prior to modifying plan
    Set<PTOperator> deps = this.getDependents(currentPartitionMap.values());
    this.undeployOpers.addAll(deps);
    // dependencies need redeploy, except operators excluded in remove
    this.deployOpers.addAll(deps);

    // plan updates start here, after all changes were identified
    // remove obsolete operators first, any freed resources
    // can subsequently be used for new/modified partitions
    List<PTOperator> copyPartitions = Lists.newArrayList(currentMapping.partitions);
    // remove deprecated partitions from plan
    for (PTOperator p : currentPartitionMap.values()) {
      copyPartitions.remove(p);
      removePartition(p, currentMapping);
      operatorIdToPartition.remove(p.getId());
    }
    currentMapping.partitions = copyPartitions;

    // add new operators after cleanup complete

    for (Partition<Operator> newPartition : addedPartitions) {
      // new partition, add to plan
      PTOperator p = addPTOperator(currentMapping, newPartition, minCheckpoint);
      operatorIdToPartition.put(p.getId(), newPartition);

      // handle parallel partition
      Stack<OperatorMeta> pending = new Stack<LogicalPlan.OperatorMeta>();
      pending.addAll(currentMapping.parallelPartitions);
      pendingLoop:
      while (!pending.isEmpty()) {
        OperatorMeta ppMeta = pending.pop();
        for (StreamMeta s : ppMeta.getInputStreams().values()) {
          if (currentMapping.parallelPartitions.contains(s.getSource().getOperatorWrapper()) && pending.contains(s.getSource().getOperatorWrapper())) {
            pending.push(ppMeta);
            pending.remove(s.getSource().getOperatorWrapper());
            pending.push(s.getSource().getOperatorWrapper());
            continue pendingLoop;
          }
        }
        LOG.debug("Adding to parallel partition {}", ppMeta);
        // even though we don't track state for parallel partitions
        // set activation windowId to confirm to upstream checkpoints
        addPTOperator(this.logicalToPTOperator.get(ppMeta), null, minCheckpoint);
      }
    }

    updateStreamMappings(currentMapping);
    for (OperatorMeta pp : currentMapping.parallelPartitions) {
      updateStreamMappings(this.logicalToPTOperator.get(pp));
    }

    deployChanges();
    this.ctx.recordEventAsync(new StramEvent.PartitionEvent(currentMapping.logicalOperator.getName(), currentPartitions.size(), newPartitions.size()));

    if (currentMapping.logicalOperator.getOperator() instanceof Partitionable.PartitionAware) {
      @SuppressWarnings({ "unchecked", "rawtypes" })
      Partitionable.PartitionAware<Operator> partitionable = (Partitionable.PartitionAware)currentMapping.logicalOperator.getOperator();
      partitionable.partitioned(operatorIdToPartition);
    }

  }

  private void updateStreamMappings(PMapping m) {
    for (Map.Entry<OutputPortMeta, StreamMeta> opm : m.logicalOperator.getOutputStreams().entrySet()) {
      StreamMapping ug = m.outputStreams.get(opm.getKey());
      if (ug == null) {
        ug = new StreamMapping(opm.getValue(), this);
        m.outputStreams.put(opm.getKey(), ug);
      }
      LOG.debug("update stream mapping for {} {}", opm.getKey().getOperatorWrapper(), opm.getKey().getPortName());
      ug.setSources(m.partitions);
      ug.redoMapping();
    }

    for (Map.Entry<InputPortMeta, StreamMeta> ipm : m.logicalOperator.getInputStreams().entrySet()) {
      PMapping sourceMapping = this.logicalToPTOperator.get(ipm.getValue().getSource().getOperatorWrapper());

      if (ipm.getKey().getValue(PortContext.PARTITION_PARALLEL)) {
        if (sourceMapping.partitions.size() < m.partitions.size()) {
          throw new AssertionError("Number of partitions don't match in parallel mapping");
        }
        for (int i=0; i<m.partitions.size(); i++) {
          PTOperator oper = m.partitions.get(i);
          PTOperator sourceOper = sourceMapping.partitions.get(i);
          for (PTOutput sourceOut : sourceOper.outputs) {
            if (sourceOut.logicalStream == ipm.getValue()) {
              PTInput input = new PTInput(ipm.getKey().getPortName(), ipm.getValue(), oper, null, sourceOut);
              oper.inputs.add(input);
            }
          }
        }
      } else {
        // TODO: we have potentially changed the input operators, refresh stream mapping
        StreamMapping ug = sourceMapping.outputStreams.get(ipm.getValue().getSource());
        if (ug == null) {
          ug = new StreamMapping(ipm.getValue(), this);
          m.outputStreams.put(ipm.getValue().getSource(), ug);
        }
        LOG.debug("update upstream stream mapping for {} {}", sourceMapping.logicalOperator, ipm.getValue().getSource().getPortName());
        ug.setSources(sourceMapping.partitions);
        ug.redoMapping();
      }
    }

  }

  public void deployChanges() {
    Set<PTContainer> newContainers = Sets.newHashSet();
    Set<PTContainer> releaseContainers = Sets.newHashSet();
    assignContainers(newContainers, releaseContainers);
    this.undeployOpers.removeAll(newOpers.keySet());
    //make sure all the new operators are included in deploy operator list
    this.deployOpers.addAll(this.newOpers.keySet());
    // include downstream dependencies of affected operators into redeploy
    Set<PTOperator> deployOperators = this.getDependents(this.deployOpers);
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

    for (Map.Entry<PTOperator, Operator> operEntry : this.newOpers.entrySet()) {

      PTOperator oper = operEntry.getKey();
      long activationWindowId = getActivationWindowId(operEntry.getKey());
      initCheckpoint(oper, operEntry.getValue(), activationWindowId);

      if (mxnUnifiers.contains(operEntry.getKey())) {
        // MxN unifiers are assigned with the downstream operator
        continue;
      }

      PTContainer newContainer = null;
      // check for existing inline set
      for (PTOperator inlineOper : oper.getGrouping(Locality.CONTAINER_LOCAL).getOperatorSet()) {
        if (inlineOper.container != null) {
          newContainer = inlineOper.container;
          break;
        }
      }

      if (newContainer == null) {
        // find container
        newContainer = findContainer(oper);
        if (newContainer == null) {
          // get new container
          LOG.debug("New container for: " + oper);
          newContainer = new PTContainer(this);
          containers.add(newContainer);
          newContainers.add(newContainer);
        }
      }

      setContainer(oper, newContainer);
    }

    // release containers that are no longer used
    for (PTContainer c : this.containers) {
      if (c.operators.isEmpty()) {
        LOG.debug("Container {} to be released", c);
        releaseContainers.add(c);
        containers.remove(c);
      }
    }
  }

  private void initCheckpoint(PTOperator oper, Operator oo, long windowId) {
    if (windowId == 0) {
      throw new AssertionError("Invalid checkpoint window id: " + oper);
    }

    try {
      LOG.debug("Writing activation checkpoint {} {} {}", windowId, oper, oo);
      OutputStream stream = ctx.getStorageAgent().getSaveStream(oper.id, windowId);
      Node.storeOperator(stream, oo);
      stream.close();
    } catch (IOException e) {
      // inconsistent state, no recovery option, requires shutdown
      throw new IllegalStateException("Failed to write operator state after partition change " + oper, e);
    }
    oper.setRecoveryCheckpoint(windowId);
    if (windowId != OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID) {
      oper.checkpointWindows.add(windowId);
    }
  }

  public Operator loadOperator(PTOperator oper) {
    try {
      LOG.debug("Loading state for {}", oper);
      InputStream is = ctx.getStorageAgent().getLoadStream(oper.id, oper.recoveryCheckpoint);
      if (is == null) {
        throw new AssertionError(String.format("Cannot read state for %s %s", oper.id, oper.recoveryCheckpoint));
      }
      try {
        return Node.retrieveOperatorWrapper(is).operator;
      } finally {
        is.close();
      }
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
  private long getActivationWindowId(PTOperator oper)
  {
    if (oper.recoveryCheckpoint == 0 && oper.checkpointWindows.isEmpty()) {
      long activationWindowId = OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID;
      for (PTInput input : oper.inputs) {
        PTOperator sourceOper = input.source.source;
        if (sourceOper.checkpointWindows.isEmpty()) {
          getActivationWindowId(sourceOper);
        }
        activationWindowId = Math.max(activationWindowId, sourceOper.getRecentCheckpoint());
      }
      return activationWindowId;
    }
    return oper.recoveryCheckpoint;
  }

  /**
   * Remove a partition that was reported as idle by the execution layer.
   * Since the end stream tuple is propagated to the downstream operators,
   * there is no need to undeploy/redeploy them as part of this operation.
   * @param p
   */
  public void removeIdlePartition(PTOperator p)
  {
    PMapping currentMapping = this.logicalToPTOperator.get(p.logicalNode);
    List<PTOperator> copyPartitions = Lists.newArrayList(currentMapping.partitions);
    copyPartitions.remove(p);
    removePartition(p, currentMapping);
    currentMapping.partitions = copyPartitions;
    deployChanges();
  }

  /**
   * Remove the given partition with any associated parallel partitions and
   * per-partition outputStreams.
   *
   * @param oper
   * @return
   */
  private void removePartition(PTOperator oper, PMapping operatorMapping) {

    // remove any parallel partition
    for (PTOutput out : oper.outputs) {
      // copy list as it is modified by recursive remove
      for (PTInput in : Lists.newArrayList(out.sinks)) {
        for (LogicalPlan.InputPortMeta im : in.logicalStream.getSinks()) {
          PMapping m = this.logicalToPTOperator.get(im.getOperatorWrapper());
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

    // per partition merge operators
    if (!oper.upstreamMerge.isEmpty()) {
      for (PTOperator unifier : oper.upstreamMerge.values()) {
        removePTOperator(unifier);
      }
    }
  }

  private PTContainer findContainer(PTOperator oper) {
    // TODO: find container based on utilization
    for (PTContainer c : this.containers) {
      if (c.operators.isEmpty() && c.getState() == PTContainer.State.ACTIVE) {
        LOG.debug("Reusing existing container {} for {}", c, oper);
        return c;
      }
    }
    return null;
  }

  private PTOperator addPTOperator(PMapping nodeDecl, Partition<?> partition, long activationWindowId) {
    String host = null;
    if(partition != null){
     host = partition.getAttributes().get(OperatorContext.LOCALITY_HOST);
    }
    if(host == null){
     host = nodeDecl.logicalOperator.getValue(OperatorContext.LOCALITY_HOST);
    }

    PTOperator oper = newOperator(nodeDecl.logicalOperator, nodeDecl.logicalOperator.getName());
    // output port objects
    for (Map.Entry<LogicalPlan.OutputPortMeta, StreamMeta> outputEntry : nodeDecl.logicalOperator.getOutputStreams().entrySet()) {
      setupOutput(nodeDecl, oper, outputEntry);
    }

    oper.recoveryCheckpoint = activationWindowId;
    if (partition != null) {
      oper.setPartitionKeys(partition.getPartitionKeys());
    }
    nodeDecl.addPartition(oper);
    this.newOpers.put(oper, partition != null ? partition.getPartitionedInstance() : nodeDecl.logicalOperator.getOperator());

    //
    // update locality
    //
    setLocalityGrouping(nodeDecl, oper, inlinePrefs, Locality.CONTAINER_LOCAL,host);
    setLocalityGrouping(nodeDecl, oper, localityPrefs, Locality.NODE_LOCAL,host);

    return oper;
  }

  /**
   * Create output port mapping for given operator and port.
   * Occurs when adding new partition or new logical stream.
   * Does nothing if source was already setup (on add sink to existing stream).
   * @param mapping
   * @param pOperator
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

  PTOperator newOperator(OperatorMeta om, String name) {
    PTOperator oper = new PTOperator(this, idSequence.incrementAndGet(), name);
    allOperators.put(oper.id, oper);
    oper.logicalNode = om;
    oper.inputs = new ArrayList<PTInput>();
    oper.outputs = new ArrayList<PTOutput>();

    this.ctx.recordEventAsync(new StramEvent.CreateOperatorEvent(oper.getName(), oper.getId()));

    return oper;
  }

  private void setLocalityGrouping(PMapping pnodes, PTOperator newOperator, LocalityPrefs localityPrefs, Locality ltype,String host) {

    HostOperatorSet grpObj = newOperator.getGrouping(ltype);
    if(host!= null) {
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
            s.addAll(localPM.partitions.get(pnodes.partitions.size()-1).getGrouping(ltype).getOperatorSet());
          }
        } else {
          for (PTOperator otherNode : localPM.partitions) {
            s.addAll(otherNode.getGrouping(ltype).getOperatorSet());
          }
        }
      }
      for (PTOperator localOper : s) {
        if(grpObj.getHost() == null){
          grpObj.setHost(localOper.groupings.get(ltype).getHost());
         }
        localOper.groupings.put(ltype, grpObj);

      }
    }
  }

  void removePTOperator(PTOperator oper) {
    LOG.debug("Removing operator " + oper);
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
    // remove checkpoint states
    try {
      synchronized (oper.checkpointWindows) {
        for (long checkpointWindowId : oper.checkpointWindows) {
          ctx.getStorageAgent().delete(oper.id, checkpointWindowId);
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

  public PlanContext getContext() {
    return ctx;
  }

  public LogicalPlan getDAG() {
    return this.dag;
  }

  public List<PTContainer> getContainers() {
    return this.containers;
  }

  public Map<Integer, PTOperator> getAllOperators() {
    return this.allOperators;
  }

  /**
   * Get the partitions for the logical operator.
   * Partitions represent instances of the operator and do not include any unifiers.
   * @param logicalOperator
   * @return
   */
  public List<PTOperator> getOperators(OperatorMeta logicalOperator) {
    return this.logicalToPTOperator.get(logicalOperator).partitions;
  }

  public boolean hasMapping(OperatorMeta om) {
    return this.logicalToPTOperator.containsKey(om);
  }

  // used for testing only
  @VisibleForTesting
  public List<PTOperator> getMergeOperators(OperatorMeta logicalOperator) {
    List<PTOperator> opers = Lists.newArrayList();
    for (StreamMapping ug : this.logicalToPTOperator.get(logicalOperator).outputStreams.values()) {
      ug.addTo(opers);
    }
    return opers;
  }

  protected List<OperatorMeta> getRootOperators() {
    return dag.getRootOperators();
  }

  private void getDeps(PTOperator operator, Set<PTOperator> visited) {
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
    Set<PTOperator> visited = new LinkedHashSet<PTOperator>();
    if (operators != null) {
      for (PTOperator operator: operators) {
        getDeps(operator, visited);
      }
    }
    return visited;
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
      PMapping m = logicalToPTOperator.get(e.getValue().getSource().getOperatorWrapper());
      if (e.getKey().getValue(PortContext.PARTITION_PARALLEL).equals(true)) {
        // operator partitioned with upstream
        if (upstreamPartitioned != null) {
          // need to have common root
          if (!upstreamPartitioned.parallelPartitions.contains(m.logicalOperator)) {
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
      for (int i=0; i<upstreamPartitioned.partitions.size(); i++) {
        // TODO: the initial checkpoint has to be derived from upstream operators
        addPTOperator(pnodes, null, OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID);
      }
    } else {
      initPartitioning(pnodes);
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
      OperatorMeta om = ipm.getOperatorWrapper();
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
    PMapping m = this.logicalToPTOperator.get(sm.getSource().getOperatorWrapper());
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
    for (Map.Entry<LogicalPlan.InputPortMeta, StreamMeta> inputEntry : ipm.getOperatorWrapper().getInputStreams().entrySet()) {
      if (inputEntry.getKey() == ipm) {
        // initialize outputs for existing operators
        for (Map.Entry<LogicalPlan.OutputPortMeta, StreamMeta> outputEntry : inputEntry.getValue().getSource().getOperatorWrapper().getOutputStreams().entrySet()) {
          PMapping sourceOpers = this.logicalToPTOperator.get(outputEntry.getKey().getOperatorWrapper());
          for (PTOperator oper : sourceOpers.partitions) {
            setupOutput(sourceOpers, oper, outputEntry); // idempotent
            undeployOpers.add(oper);
            deployOpers.add(oper);
          }
        }
        PMapping m = this.logicalToPTOperator.get(ipm.getOperatorWrapper());
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

  public void onStatusUpdate(PTOperator oper)
  {
    for (StatsListener l : oper.statsListeners) {
      StatsListener.Response rsp = l.processStats(oper.stats);
      if (rsp != null) {
        // TODO: repartition delay needs to come out of the listener
        oper.loadIndicator = rsp.loadIndicator;
        if (rsp.repartitionRequired) {
          final OperatorMeta om = oper.getOperatorMeta();
          // concurrent heartbeat processing
          if (this.pendingRepartition.putIfAbsent(om, om) != null) {
            LOG.debug("Skipping repartitioning for {} load {}", oper, oper.loadIndicator);
          } else {
            LOG.debug("Scheduling repartitioning for {} {}", oper, oper.loadIndicator);
            // hand over to monitor thread
            Runnable r = new Runnable() {
              @Override
              public void run() {
                pendingRepartition.remove(om);
                redoPartitions(logicalToPTOperator.get(om));
              }
            };
            ctx.dispatch(r);
          }
        }
      }
    }
  }
}

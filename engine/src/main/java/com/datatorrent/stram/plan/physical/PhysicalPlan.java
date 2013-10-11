/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan.physical;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.PartitionableOperator;
import com.datatorrent.api.PartitionableOperator.Partition;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.stram.EventRecorder;
import com.datatorrent.stram.FSEventRecorder;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.physical.OperatorPartitions.PartitionImpl;
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
public class PhysicalPlan {

  private final static Logger LOG = LoggerFactory.getLogger(PhysicalPlan.class);

  public static interface StatsHandler {
    // TODO: handle stats generically
    public void onThroughputUpdate(PTOperator operatorInstance, long tps);
    public void onCpuPercentageUpdate(PTOperator operatorInstance, double percentage);
  }

  /**
   * Handler for partition load check.
   * Used when throughput monitoring is configured.
   */
  public static class PartitionLoadWatch implements StatsHandler {
    public long evalIntervalMillis = 30*1000;
    private final long tpsMin;
    private final long tpsMax;
    private long lastEvalMillis;
    private long lastTps = 0;

    private final PMapping m;

    private PartitionLoadWatch(PMapping mapping, long min, long max) {
      this.m = mapping;
      this.tpsMin = min;
      this.tpsMax = max;
    }

    protected PartitionLoadWatch(PMapping mapping) {
      this(mapping, 0, 0);
    }

    protected int getLoadIndicator(PTOperator operator, long tps) {
      if ((tps < tpsMin && lastTps != 0) || tps > tpsMax) {
        lastTps = tps;
        return (tps < tpsMin) ? -1 : 1;
      }
      lastTps = tps;
      return 0;
    }

    @Override
    public void onThroughputUpdate(final PTOperator operatorInstance, long tps) {
      //LOG.debug("onThroughputUpdate {} {}", operatorInstance, tps);
      operatorInstance.loadIndicator = getLoadIndicator(operatorInstance, tps);
      if (operatorInstance.loadIndicator != 0) {
        if (lastEvalMillis < (System.currentTimeMillis() - evalIntervalMillis)) {
          lastEvalMillis = System.currentTimeMillis();
          synchronized (m) {
            // concurrent heartbeat processing
            if (m.shouldRedoPartitions) {
              LOG.debug("Skipping partition update for {} tps: {}", operatorInstance, tps);
              return;
            }
            m.shouldRedoPartitions = true;
            LOG.debug("Scheduling partitioning update for {} {}", m.logicalOperator, operatorInstance.loadIndicator);
            // hand over to monitor thread
            Runnable r = new Runnable() {
              @Override
              public void run() {
                operatorInstance.getPlan().redoPartitions(m);
                synchronized (m) {
                  m.shouldRedoPartitions = false;
                }
              }
            };
            operatorInstance.getPlan().ctx.dispatch(r);
          }
        }
      }
    }

    @Override
    public void onCpuPercentageUpdate(final PTOperator operatorInstance, double percentage) {
      // not implemented yet
    }
  }

  /**
   * Determine operators that should be deployed into the execution environment.
   * Operators can be deployed once all containers are running and any pending
   * undeploy operations are complete.
   * @param c
   * @return
   */
  public Set<PTOperator> getOperatorsForDeploy(PTContainer c) {
    for (PTContainer otherContainer : this.containers) {
      if (otherContainer != c && (otherContainer.state != PTContainer.State.ACTIVE || !otherContainer.pendingUndeploy.isEmpty())) {
        LOG.debug("{}({}) unsatisfied dependency: {}({}) undeploy: {}", new Object[] {c.containerId, c.state, otherContainer.containerId, otherContainer.state, otherContainer.pendingUndeploy.size()});
        return Collections.emptySet();
      }
    }
    return c.pendingDeploy;
  }

  private final AtomicInteger idSequence = new AtomicInteger();
  final AtomicInteger containerSeq = new AtomicInteger();
  private LinkedHashMap<OperatorMeta, PMapping> logicalToPTOperator = new LinkedHashMap<OperatorMeta, PMapping>();
  private final List<PTContainer> containers = new CopyOnWriteArrayList<PTContainer>();
  private final LogicalPlan dag;
  private final PlanContext ctx;
  private int maxContainers = 1;
  private final LocalityPrefs localityPrefs = new LocalityPrefs();
  private final LocalityPrefs inlinePrefs = new LocalityPrefs();

  final Set<PTOperator> deployOpers = Sets.newHashSet();
  final Set<PTOperator> newOpers = Sets.newHashSet();
  final Set<PTOperator> undeployOpers = Sets.newHashSet();

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
    public void recordEventAsync(EventRecorder.Event ev);

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

  }

  /**
   * The logical operator with physical plan info tagged on.
   */
  public static class PMapping {
    private PMapping(OperatorMeta om) {
      this.logicalOperator = om;
    }

    final private OperatorMeta logicalOperator;
    private List<PTOperator> partitions = new LinkedList<PTOperator>();
    private Map<LogicalPlan.OutputPortMeta, StreamMapping> outputStreams = Maps.newHashMap();
    volatile private boolean shouldRedoPartitions = false;
    private List<StatsHandler> statsHandlers;

    /**
     * Operators that form a parallel partition
     */
    private Set<OperatorMeta> parallelPartitions = Sets.newHashSet();

    private void addPartition(PTOperator p) {
      partitions.add(p);
      p.statsMonitors = this.statsHandlers;
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

    private boolean isPartitionable() {
      int partitionCnt = logicalOperator.attrValue(OperatorContext.INITIAL_PARTITION_COUNT, 0);
      return (partitionCnt > 0);
    }

    @Override
    public String toString() {
      return logicalOperator.toString();
    }
  }

  private class LocalityPref {
    String host;
    Set<PMapping> operators = Sets.newHashSet();
  }

  /**
   * Group logical operators by locality constraint. Used to derive locality
   * groupings for physical operators, which are used when assigning containers
   * and requesting resources from the scheduler.
   */
  private class LocalityPrefs {
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
          Set<PTOperator> inlineSet = oper.getGrouping(Locality.CONTAINER_LOCAL);
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
        mEntry.getValue().container = container;
        container.operators.add(mEntry.getValue());
      }
    }
  }

  private void initPartitioning(PMapping m)  {
    /*
     * partitioning is enabled through initial count attribute.
     * if the attribute is not present or set to zero, partitioning is off
     */
    int partitionCnt = m.logicalOperator.attrValue(OperatorContext.INITIAL_PARTITION_COUNT, 0);
    if (partitionCnt == 0) {
      throw new AssertionError("operator not partitionable " + m.logicalOperator);
    }

    Operator operator = m.logicalOperator.getOperator();
    Collection<Partition<?>> partitions = new ArrayList<Partition<?>>(1);
    if (operator instanceof PartitionableOperator) {
      // operator to provide initial partitioning
      partitions.add(new PartitionImpl(operator));
      partitions = ((PartitionableOperator)operator).definePartitions(partitions, partitionCnt - 1);
    }
    else {
      partitions = new OperatorPartitions.DefaultPartitioner().defineInitialPartitions(m.logicalOperator, partitionCnt);
    }

    if (partitions == null || partitions.isEmpty()) {
      throw new IllegalArgumentException("PartitionableOperator must return at least one partition: " + m.logicalOperator);
    }

    int minTps = m.logicalOperator.attrValue(OperatorContext.PARTITION_TPS_MIN, 0);
    int maxTps = m.logicalOperator.attrValue(OperatorContext.PARTITION_TPS_MAX, 0);
    if (maxTps > minTps) {
      // monitor load
      if (m.statsHandlers == null) {
        m.statsHandlers = new ArrayList<StatsHandler>(1);
      }
      m.statsHandlers.add(new PartitionLoadWatch(m, minTps, maxTps));
    }

    String handlers = m.logicalOperator.attrValue(OperatorContext.PARTITION_STATS_HANDLER, null);
    if (handlers != null) {
      if (m.statsHandlers == null) {
        m.statsHandlers = new ArrayList<StatsHandler>(1);
      }
      Class<? extends StatsHandler> shClass = StramUtils.classForName(handlers, StatsHandler.class);
      final StatsHandler sh;
      if (PartitionLoadWatch.class.isAssignableFrom(shClass)) {
        try {
          sh = shClass.getConstructor(m.getClass()).newInstance(m);
        }
        catch (Exception e) {
          throw new RuntimeException("Failed to create custom partition load handler.", e);
        }
      }
      else {
        sh = StramUtils.newInstance(shClass);
      }
      m.statsHandlers.add(sh);
    }

    // create operator instance per partition
    for (Partition<?> p: partitions) {
      addPTOperator(m, p);
    }
    updateStreamMappings(m);

  }

  private void redoPartitions(PMapping currentMapping) {
    // collect current partitions with committed operator state
    // those will be needed by the partitioner for split/merge
    List<PTOperator> operators = currentMapping.partitions;
    List<PartitionImpl> currentPartitions = new ArrayList<PartitionImpl>(operators.size());
    Map<Partition<?>, PTOperator> currentPartitionMap = new HashMap<Partition<?>, PTOperator>(operators.size());

    final Collection<Partition<?>> newPartitions;
    long minCheckpoint = -1;

    for (PTOperator pOperator : operators) {
      Partition<?> p = pOperator.partition;
      if (p == null) {
        throw new AssertionError("Null partition: " + pOperator);
      }
      // load operator state
      // the partitioning logic will have the opportunity to merge/split state
      // since partitions checkpoint at different windows, processing for new or modified
      // partitions will start from earliest checkpoint found (at least once semantics)
      Operator partitionedOperator = p.getOperator();
      if (pOperator.recoveryCheckpoint != 0) {
        try {
          LOG.debug("Loading state for {}", pOperator);
          InputStream is = ctx.getStorageAgent().getLoadStream(pOperator.id, pOperator.recoveryCheckpoint);
          if (is != null) {
            partitionedOperator = Node.retrieveOperatorWrapper(is).operator;
          }
        } catch (IOException e) {
          LOG.warn("Failed to read partition state for " + pOperator, e);
          return; // TODO: emit to event log
        }
      }

      if (minCheckpoint < 0) {
        minCheckpoint = pOperator.recoveryCheckpoint;
      } else {
        minCheckpoint = Math.min(minCheckpoint, pOperator.recoveryCheckpoint);
      }

      // assume it does not matter which operator instance's port objects are referenced in mapping
      PartitionImpl partition = new PartitionImpl(partitionedOperator, p.getPartitionKeys(), pOperator.loadIndicator);
      currentPartitions.add(partition);
      currentPartitionMap.put(partition, pOperator);
    }

    for (Map.Entry<Partition<?>, PTOperator> e : currentPartitionMap.entrySet()) {
      LOG.debug("partition load: {} {} {}", new Object[] {e.getValue(), e.getKey().getPartitionKeys(), e.getKey().getLoad()});
    }
    if (currentMapping.logicalOperator.getOperator() instanceof PartitionableOperator) {
      // would like to know here how much more capacity we have here so that definePartitions can act accordingly.
      final int incrementalCapacity = 0;
      newPartitions = ((PartitionableOperator)currentMapping.logicalOperator.getOperator()).definePartitions(currentPartitions, incrementalCapacity);
    } else {
      if (!currentMapping.logicalOperator.getInputStreams().isEmpty()) {
        newPartitions = new OperatorPartitions.DefaultPartitioner().repartition(currentPartitions);
      } else {
        newPartitions = OperatorPartitions.DefaultPartitioner.repartitionInputOperator(currentPartitions);
      }
    }

    if (newPartitions.isEmpty()) {
      LOG.warn("Empty partition list after repartition: {}", currentMapping.logicalOperator);
    }

    List<Partition<?>> addedPartitions = new ArrayList<Partition<?>>();
    // determine modifications of partition set, identify affected operator instance(s)
    for (Partition<?> newPartition : newPartitions) {
      PTOperator op = currentPartitionMap.remove(newPartition);
      if (op == null) {
        addedPartitions.add(newPartition);
      } else {
        // check whether mapping was changed
        for (PartitionImpl pi : currentPartitions) {
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
    // dependencies need redeploy, except operators that excluded from the set in remove
    this.deployOpers.addAll(deps);

    // plan updates start here, after all changes were identified
    // remove obsolete operators first, any freed resources
    // can subsequently be used for new/modified partitions
    PMapping newMapping = new PMapping(currentMapping.logicalOperator);
    newMapping.partitions.addAll(currentMapping.partitions);
    newMapping.outputStreams.putAll(currentMapping.outputStreams);
    newMapping.statsHandlers = currentMapping.statsHandlers;

    // remove deprecated partitions from plan
    for (PTOperator p : currentPartitionMap.values()) {
      newMapping.partitions.remove(p);
      removePartition(p, currentMapping.parallelPartitions);
    }

    // keep mapping reference as that's where stats monitors point to
    currentMapping.outputStreams = newMapping.outputStreams;
    currentMapping.partitions = newMapping.partitions;

    // add new operators after cleanup complete

    for (Partition<?> newPartition : addedPartitions) {
      // new partition, add operator instance
      PTOperator p = addPTOperator(currentMapping, newPartition);
      initCheckpoint(p, newPartition.getOperator(), minCheckpoint);

      for (PTOperator mergeOper : p.upstreamMerge.values()) {
        // TODO: remove as unifier is now handled in stream mapping?
        this.undeployOpers.add(mergeOper);
        this.deployOpers.add(mergeOper);
        initCheckpoint(mergeOper, mergeOper.unifier, minCheckpoint);
      }

      // handle parallel partition
      Stack<OperatorMeta> pending = new Stack<LogicalPlan.OperatorMeta>();
      pending.addAll(currentMapping.parallelPartitions);
      pendingLoop:
      while (!pending.isEmpty()) {
        OperatorMeta pp = pending.pop();
        for (StreamMeta s : pp.getInputStreams().values()) {
          if (currentMapping.parallelPartitions.contains(s.getSource().getOperatorWrapper()) && pending.contains(s.getSource().getOperatorWrapper())) {
            pending.push(pp);
            pending.remove(s.getSource().getOperatorWrapper());
            pending.push(s.getSource().getOperatorWrapper());
            continue pendingLoop;
          }
        }
        LOG.debug("Adding to parallel partition {}", pp);
        PTOperator ppOper = addPTOperator(this.logicalToPTOperator.get(pp), null);

        // even though we don't track state for parallel partitions
        // set recovery windowId to confirm with upstream checkpoints
        initCheckpoint(ppOper, pp.getOperator(), minCheckpoint);
      }
    }

    updateStreamMappings(currentMapping);
    for (OperatorMeta pp : currentMapping.parallelPartitions) {
      updateStreamMappings(this.logicalToPTOperator.get(pp));
    }

    deployChanges();

    EventRecorder.Event ev = new FSEventRecorder.Event("partition");
    ev.addData("operatorName", currentMapping.logicalOperator.getName());
    ev.addData("currentNumPartitions", currentPartitions.size());
    ev.addData("newNumPartitions", newPartitions.size());
    this.ctx.recordEventAsync(ev);

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

      if (ipm.getKey().attrValue(PortContext.PARTITION_PARALLEL, false)) {
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
    this.undeployOpers.removeAll(newOpers);
    // include downstream dependencies of affected operators into redeploy
    Set<PTOperator> deployOperators = this.getDependents(this.deployOpers);
    ctx.deploy(releaseContainers, this.undeployOpers, newContainers, deployOperators);
    this.newOpers.clear();
    this.deployOpers.clear();
    this.undeployOpers.clear();
  }

  private void assignContainers(Set<PTContainer> newContainers, Set<PTContainer> releaseContainers) {
    // assign containers to new operators
    for (PTOperator oper : this.newOpers) {

      initCheckpoint(oper);

      PTContainer newContainer = null;
      // check for existing inline set
      for (PTOperator inlineOper : oper.getGrouping(Locality.CONTAINER_LOCAL)) {
        if (inlineOper.container != null) {
          newContainer = inlineOper.container;
          break;
        }
      }

      if (newContainer != null) {
        setContainer(oper, newContainer);
        continue;
      }

      // find container
      PTContainer c = null;
      if (c == null) {
        c = findContainer(oper);
        if (c == null) {
          // get new container
          LOG.debug("New container for partition: " + oper);
          c = new PTContainer(this);
          containers.add(c);
          newContainers.add(c);
        }
      }
      setContainer(oper, c);
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

  private void initCheckpoint(PTOperator partition, Operator operator, long windowId) {
    partition.recoveryCheckpoint = windowId;
    if (windowId > 0) {
      partition.checkpointWindows.add(windowId);
      try {
        OutputStream stream = ctx.getStorageAgent().getSaveStream(partition.id, windowId);
        Node.storeOperator(stream, operator);
        stream.close();
      } catch (IOException e) {
        // inconsistent state, no recovery option, requires shutdown
        throw new IllegalStateException("Failed to write operator state after partition change " + partition, e);
      }
    }
  }

  /**
   * Initialize the activation checkpoint for the given operator.
   * Recursively traverses inputs until an existing checkpoint or root operator is found.
   * NoOp when already initialized.
   * @param oper
   */
  private void initCheckpoint(PTOperator oper)
  {
    if (oper.checkpointWindows.isEmpty()) {
      long activationWindowId = 0;
      for (PTInput input : oper.inputs) {
        PTOperator sourceOper = input.source.source;
        if (sourceOper.checkpointWindows.isEmpty()) {
          initCheckpoint(sourceOper);
        }
        activationWindowId = Math.max(activationWindowId, sourceOper.getRecentCheckpoint());
      }
      if (activationWindowId > 0) {
        Operator oo = oper.partition != null ? oper.partition.getOperator() : oper.logicalNode.getOperator();
        initCheckpoint(oper, oo, activationWindowId);
      }
    }
  }

  /**
   * Remove the given partition with any associated parallel partitions and
   * per-partition outputStreams.
   *
   * @param oper
   * @return
   */
  private void removePartition(PTOperator oper, Set<LogicalPlan.OperatorMeta> parallelPartitions) {

    // remove any parallel partition
    for (PTOutput out : oper.outputs) {
      // copy list as it is modified by recursive remove
      for (PTInput in : Lists.newArrayList(out.sinks)) {
        for (LogicalPlan.InputPortMeta im : in.logicalStream.getSinks()) {
          PMapping m = this.logicalToPTOperator.get(im.getOperatorWrapper());
          if (m.parallelPartitions == parallelPartitions) {
            // associated operator parallel partitioned
            removePartition(in.target, parallelPartitions);
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

  private PTOperator addPTOperator(PMapping nodeDecl, Partition<?> partition) {
    PTOperator oper = createInstance(nodeDecl, partition);
    nodeDecl.addPartition(oper);
    this.newOpers.add(oper);
    this.deployOpers.add(oper);

    //
    // update locality
    //
    setLocalityGrouping(nodeDecl, oper, inlinePrefs, Locality.CONTAINER_LOCAL);
    setLocalityGrouping(nodeDecl, oper, localityPrefs, Locality.NODE_LOCAL);

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
    oper.logicalNode = om;
    oper.inputs = new ArrayList<PTInput>();
    oper.outputs = new ArrayList<PTOutput>();
    return oper;
  }

  private PTOperator createInstance(PMapping mapping, Partition<?> partition) {
    PTOperator pOperator = new PTOperator(this, idSequence.incrementAndGet(), mapping.logicalOperator.getName());
    pOperator.logicalNode = mapping.logicalOperator;
    pOperator.inputs = new ArrayList<PTInput>();
    pOperator.outputs = new ArrayList<PTOutput>();
    pOperator.partition = partition;

    // output port objects
    for (Map.Entry<LogicalPlan.OutputPortMeta, StreamMeta> outputEntry : mapping.logicalOperator.getOutputStreams().entrySet()) {
      setupOutput(mapping, pOperator, outputEntry);
    }
    return pOperator;
  }

  private void setLocalityGrouping(PMapping pnodes, PTOperator newOperator, LocalityPrefs localityPrefs, Locality ltype) {

    Set<PTOperator> s = newOperator.getGrouping(ltype);
    s.add(newOperator);
    LocalityPref loc = localityPrefs.prefs.get(pnodes);
    if (loc != null) {
      for (PMapping localPM : loc.operators) {
        if (pnodes.parallelPartitions == localPM.parallelPartitions) {
          if (localPM.partitions.size() >= pnodes.partitions.size()) {
            // apply locality setting per partition
            s.addAll(localPM.partitions.get(pnodes.partitions.size()-1).getGrouping(ltype));
          }
        } else {
          for (PTOperator otherNode : localPM.partitions) {
            s.addAll(otherNode.getGrouping(ltype));
          }
        }
      }
      for (PTOperator localOper : s) {
        localOper.groupings.put(ltype, s);
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
  }

  public LogicalPlan getDAG() {
    return this.dag;
  }

  public List<PTContainer> getContainers() {
    return this.containers;
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

  public List<PTOperator> getAllOperators() {
    List<PTOperator> list = new ArrayList<PTOperator>();
    for (PTContainer c : this.containers) {
      list.addAll(c.getOperators());
    }
    return list;
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
  public void addLogicalOperator(OperatorMeta om)
  {
    PMapping pnodes = new PMapping(om);
    localityPrefs.add(pnodes, pnodes.logicalOperator.attrValue(OperatorContext.LOCALITY_HOST, null));

    PMapping upstreamPartitioned = null;

    for (Map.Entry<LogicalPlan.InputPortMeta, StreamMeta> e : om.getInputStreams().entrySet()) {
      PMapping m = logicalToPTOperator.get(e.getValue().getSource().getOperatorWrapper());
      if (e.getKey().attrValue(PortContext.PARTITION_PARALLEL, false).equals(true)) {
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
    if (pnodes.isPartitionable()) {
      initPartitioning(pnodes);
    } else {
      if (upstreamPartitioned != null) {
        // parallel partition
        for (int i=0; i<upstreamPartitioned.partitions.size(); i++) {
          addPTOperator(pnodes, null);
        }
      } else {
        // single instance, no partitions
        addPTOperator(pnodes, null);
      }
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
      removePartition(oper, opers.parallelPartitions);
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

}

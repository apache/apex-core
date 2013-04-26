/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Context.PortContext;
import com.malhartech.api.DAG;
import com.malhartech.api.DAG.InputPortMeta;
import com.malhartech.api.DAG.OperatorMeta;
import com.malhartech.api.DAG.StreamMeta;
import com.malhartech.api.Operator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.api.PartitionableOperator;
import com.malhartech.api.PartitionableOperator.Partition;
import com.malhartech.api.PartitionableOperator.PartitionKeys;
import com.malhartech.engine.DefaultUnifier;
import com.malhartech.engine.Operators;
import com.malhartech.engine.Operators.PortMappingDescriptor;
import com.malhartech.stram.OperatorPartitions.PartitionImpl;

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
 */
public class PhysicalPlan {

  private final static Logger LOG = LoggerFactory.getLogger(PhysicalPlan.class);

  /**
   * Common abstraction for physical DAG nodes.<p>
   * <br>
   *
   */
  public abstract static class PTComponent {
    PTContainer container;

    abstract int getId();

    /**
     *
     * @return String
     */
    abstract public String getLogicalId();

    public PTContainer getContainer() {
      return container;
    }

  }

  /**
   *
   * Representation of an input in the physical layout. A source in the DAG<p>
   * <br>
   */
  public static class PTInput {
    final DAG.StreamMeta logicalStream;
    final PTComponent target;
    final PartitionKeys partitions;
    final PTOutput source;
    final String portName;

    /**
     *
     * @param portName
     * @param logicalStream
     * @param target
     * @param partitions
     * @param source
     */
    protected PTInput(String portName, StreamMeta logicalStream, PTComponent target, PartitionKeys partitions, PTOutput source) {
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
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("target", this.target).
          append("port", this.portName).
          append("stream", this.logicalStream.getId()).
          toString();
    }

  }

  /**
   *
   * Representation of an output in the physical layout. A sink in the DAG<p>
   * <br>
   */
  public static class PTOutput {
    final DAG.StreamMeta logicalStream;
    final PTComponent source;
    final String portName;
    final PhysicalPlan plan;
    final List<PTInput> sinks;

    /**
     * Constructor
     * @param logicalStream
     * @param source
     */
    protected PTOutput(PhysicalPlan plan, String portName, StreamMeta logicalStream, PTComponent source) {
      this.plan = plan;
      this.logicalStream = logicalStream;
      this.source = source;
      this.portName = portName;
      this.sinks = new ArrayList<PTInput>();
    }

    /**
     * Determine whether downstream operators are deployed inline.
     * (all instances of the downstream operator are in the same container)
     * @return boolean
     */
    protected boolean isDownStreamInline() {
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
    protected long evalIntervalMillis = 30*1000;
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
      //LOG.debug("onThroughputUpdate " + operatorInstance);
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

  /**
   *
   * Representation of an operator in the physical layout<p>
   * <br>
   *
   */
  public static class PTOperator extends PTComponent {

    public enum State {
      NEW,
      PENDING_DEPLOY,
      ACTIVE,
      PENDING_UNDEPLOY,
      INACTIVE,
      REMOVED
    }

    PTOperator(PhysicalPlan plan, int id, String name) {
      this.plan = plan;
      this.name = name;
      this.id = id;
    }

    private State state = State.NEW;
    private final PhysicalPlan plan;
    private DAG.OperatorMeta logicalNode;
    private final int id;
    private final String name;
    Partition<?> partition;
    Operator merge;
    List<PTInput> inputs;
    List<PTOutput> outputs;
    LinkedList<Long> checkpointWindows = new LinkedList<Long>();
    long recoveryCheckpoint = 0;
    int failureCount = 0;
    int loadIndicator = 0;
    List<? extends StatsHandler> statsMonitors;

    private enum LocalityType {
      CONTAINER_LOCAL,
      NODE_LOCAL,
      RACK_LOCAL
    }

    private final Map<LocalityType, Set<PTOperator>> groupings = Maps.newHashMapWithExpectedSize(3);

    List<StreamingContainerUmbilicalProtocol.StramToNodeRequest> deployRequests = Collections.emptyList();

    final HashMap<InputPortMeta, PTOperator> upstreamMerge = new HashMap<InputPortMeta, PTOperator>();

    /**
     *
     * @return Operator
     */
    public OperatorMeta getOperatorMeta() {
      return this.logicalNode;
    }

    public State getState() {
      return state;
    }

    public void setState(State state) {
      this.state = state;
    }

    /**
     * Return the most recent checkpoint for this operator,
     * representing the last backup reported.
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
   public long getRecoveryCheckpoint() {
     return recoveryCheckpoint;
   }

    /**
     *
     * @return String
     */
    @Override
    public String getLogicalId() {
      return logicalNode.getId();
    }

    @Override
    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public PhysicalPlan getPlan() {
      return plan;
    }

    private Set<PTOperator> getGrouping(LocalityType type) {
      Set<PTOperator> s = this.groupings.get(type);
      if (s == null) {
        s = Sets.newHashSet();
        this.groupings.put(type, s);
      }
      return s;
    }

    public Set<PTOperator> getNodeLocalOperators() {
      return getGrouping(LocalityType.NODE_LOCAL);
    }

    /**
    *
    * @return String
    */
   @Override
   public String toString() {
     return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
         append("id", id).
         append("name", name).
         toString();
   }

  }

  /**
   *
   * Representation of a container for physical objects of DAG to be placed in
   * <p>
   * <br>
   * References the actual container assigned by the resource manager which
   * hosts the streaming operators in the execution layer.<br>
   * The container reference may change throughout the lifecycle of the
   * application due to failure/recovery or scheduler decisions in general. <br>
   *
   */

  public static class PTContainer {
    public enum State {
      NEW,
      ALLOCATED,
      ACTIVE,
      TIMEDOUT,
      KILLED
    }

    private State state = State.NEW;
    private int requiredMemoryMB;
    private int allocatedMemoryMB;
    private int resourceRequestPriority;

    List<PTOperator> operators = new ArrayList<PTOperator>();
    Set<PTOperator> pendingUndeploy = Collections.newSetFromMap(new ConcurrentHashMap<PTOperator, Boolean>());
    Set<PTOperator> pendingDeploy = Collections.newSetFromMap(new ConcurrentHashMap<PTOperator, Boolean>());

    String containerId; // assigned yarn container id
    String host;
    InetSocketAddress bufferServerAddress;
    int restartAttempts;
    final PhysicalPlan plan;
    private final int seq;

    PTContainer(PhysicalPlan plan) {
      this.plan = plan;
      this.seq = plan.containerSeq.incrementAndGet();
    }

    public State getState() {
      return this.state;
    }

    public void setState(State state) {
      this.state = state;
    }

    public int getRequiredMemoryMB() {
      return requiredMemoryMB;
    }

    public void setRequiredMemoryMB(int requiredMemoryMB) {
      this.requiredMemoryMB = requiredMemoryMB;
    }

    public int getAllocatedMemoryMB() {
      return allocatedMemoryMB;
    }

    public void setAllocatedMemoryMB(int allocatedMemoryMB) {
      this.allocatedMemoryMB = allocatedMemoryMB;
    }

    public int getResourceRequestPriority() {
      return resourceRequestPriority;
    }

    public void setResourceRequestPriority(int resourceRequestPriority) {
      this.resourceRequestPriority = resourceRequestPriority;
    }

    /**
     *
     * @return String
     */
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", ""+seq + "(" + this.containerId + ")").
          append("state", this.getState()).
          append("operators", this.operators).
          toString();
    }
  }

  private final AtomicInteger idSequence = new AtomicInteger();
  private final AtomicInteger containerSeq = new AtomicInteger();
  private final LinkedHashMap<OperatorMeta, PMapping> logicalToPTOperator = new LinkedHashMap<OperatorMeta, PMapping>();
  private final List<PTContainer> containers = new CopyOnWriteArrayList<PTContainer>();
  private final DAG dag;
  private final PlanContext ctx;
  private int maxContainers = 1;
  private final LocalityPrefs localityPrefs = new LocalityPrefs();
  private final LocalityPrefs inlinePrefs = new LocalityPrefs();

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
  interface PlanContext {

    /**
     * Dynamic partitioning requires access to operator state for split or merge.
     * @return
     */
    public BackupAgent getBackupAgent();

    /**
     * Request deployment change as sequence of undeploy, container start and deploy groups with dependency.
     * Called on initial plan and on dynamic changes during execution.
     */
    public void redeploy(Collection<PTOperator> undeploy, Set<PTContainer> startContainers, Collection<PTOperator> deploy);

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
    private Map<DAG.OutputPortMeta, PTOperator> mergeOperators = new HashMap<DAG.OutputPortMeta, PTOperator>();
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
      for (PTOperator out : mergeOperators.values()) {
        c.add(out);
      }
      return c;
    }

    private boolean isPartitionable() {
      int partitionCnt = logicalOperator.getAttributes().attrValue(OperatorContext.INITIAL_PARTITION_COUNT, 0);
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
  public PhysicalPlan(DAG dag, PlanContext ctx) {

    this.dag = dag;
    this.ctx = ctx;
    this.maxContainers = Math.max(dag.getMaxContainerCount(),1);
    LOG.debug("Initializing for {} containers.", this.maxContainers);

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
        // ready to look at this node
        PMapping pnodes = new PMapping(n);
        localityPrefs.add(pnodes, pnodes.logicalOperator.getAttributes().attrValue(OperatorContext.LOCALITY_HOST, null));

        PMapping upstreamPartitioned = null;

        for (Map.Entry<DAG.InputPortMeta, StreamMeta> e : n.getInputStreams().entrySet()) {
          PMapping m = logicalToPTOperator.get(e.getValue().getSource().getOperatorWrapper());
          if (e.getKey().getAttributes().attrValue(PortContext.PARTITION_PARALLEL, false).equals(true)) {
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

          if (e.getValue().isInline()) {
            inlinePrefs.setLocal(m, pnodes);
          } else if (e.getValue().isNodeLocal()) {
            localityPrefs.setLocal(m, pnodes);
          }
        }

        //
        // create operator instances
        //
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
        this.logicalToPTOperator.put(n, pnodes);
      }
    }

    // assign operators to containers
    int groupCount = 0;
    Set<PTOperator> deployOperators = Sets.newHashSet();
    for (Map.Entry<OperatorMeta, PMapping> e : logicalToPTOperator.entrySet()) {
      for (PTOperator oper : e.getValue().getAllOperators()) {
        if (oper.container == null) {
          PTContainer container = getContainer((groupCount++) % maxContainers);
          Set<PTOperator> inlineSet = oper.getGrouping(PTOperator.LocalityType.CONTAINER_LOCAL);
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
    ctx.redeploy(Collections.<PTOperator>emptySet(), Sets.newHashSet(containers), deployOperators);

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
    int partitionCnt = m.logicalOperator.getAttributes().attrValue(OperatorContext.INITIAL_PARTITION_COUNT, 0);
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

    int minTps = m.logicalOperator.getAttributes().attrValue(OperatorContext.PARTITION_TPS_MIN, 0);
    int maxTps = m.logicalOperator.getAttributes().attrValue(OperatorContext.PARTITION_TPS_MAX, 0);
    if (maxTps > minTps) {
      // monitor load
      if (m.statsHandlers == null) {
        m.statsHandlers = new ArrayList<StatsHandler>(1);
      }
      m.statsHandlers.add(new PartitionLoadWatch(m, minTps, maxTps));
    }

    String handlers = dag.getAttributes().attrValue(DAG.STRAM_STATS_HANDLER, null);
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
  }

  private void redoPartitions(PMapping currentMapping) {
    // collect current partitions with committed operator state
    // those will be needed by the partitioner for split/merge
    List<PTOperator> operators = currentMapping.partitions;
    List<PartitionImpl> currentPartitions = new ArrayList<PartitionImpl>(operators.size());
    Map<Partition<?>, PTOperator> currentPartitionMap = new HashMap<Partition<?>, PTOperator>(operators.size());

    final Collection<Partition<?>> newPartitions;
    long minCheckpoint = -1;
    PTContainer inlineContainer = null;

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
          partitionedOperator = (Operator)ctx.getBackupAgent().restore(pOperator.id, pOperator.recoveryCheckpoint, StramUtils.getNodeSerDe(null));
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

      // track existing inline deployment (publishers are not switched dynamically)
      if (inlineContainer == null) {
        for (PTInput in : pOperator.inputs) {
          if (in.source.isDownStreamInline()) {
            inlineContainer = in.source.source.container;
            break;
          }
        }
      }
    }

    for (Map.Entry<Partition<?>, PTOperator> e : currentPartitionMap.entrySet()) {
      LOG.debug("partition load: {} {} {}", new Object[] {e.getValue(), e.getKey().getPartitionKeys(), e.getKey().getLoad()});
    }
    if (currentMapping.logicalOperator.getOperator() instanceof PartitionableOperator) {
      // would like to know here how much more capacity we have here so that definePartitions can act accordingly.
      final int incrementalCapacity = 0;
      newPartitions = ((PartitionableOperator)currentMapping.logicalOperator.getOperator()).definePartitions(currentPartitions, incrementalCapacity);
    } else {
      newPartitions = new OperatorPartitions.DefaultPartitioner().repartition(currentPartitions);
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

    Set<PTOperator> undeployOperators = new HashSet<PTOperator>();
    Set<PTOperator> deployOperators = new HashSet<PTOperator>();
    // remaining entries represent deprecated partitions
    undeployOperators.addAll(currentPartitionMap.values());
    // resolve dependencies that require redeploy
    undeployOperators = this.getDependents(undeployOperators);

    // plan updates start here, after all changes were identified
    // remove obsolete operators first, any freed resources
    // can subsequently be used for new/modified partitions
    PMapping newMapping = new PMapping(currentMapping.logicalOperator);
    newMapping.partitions.addAll(currentMapping.partitions);
    newMapping.mergeOperators.putAll(currentMapping.mergeOperators);
    newMapping.statsHandlers = currentMapping.statsHandlers;

    // remove deprecated partitions from plan
    for (PTOperator p : currentPartitionMap.values()) {
      newMapping.partitions.remove(p);
      removePartition(p, currentMapping.parallelPartitions);
    }

    // keep mapping reference as that is where stats monitors point to
    currentMapping.mergeOperators = newMapping.mergeOperators;
    currentMapping.partitions = newMapping.partitions;

    // add new operators after cleanup complete
    Set<PTContainer> newContainers = Sets.newHashSet();
    Set<PTOperator> newOperators = Sets.newHashSet();

    for (Partition<?> newPartition : addedPartitions) {
      // new partition, add operator instance
      PTOperator p = addPTOperator(currentMapping, newPartition);
      deployOperators.add(p);
      newOperators.add(p);

      // handle parallel partition
      Stack<OperatorMeta> pending = new Stack<DAG.OperatorMeta>();
      pending.addAll(currentMapping.parallelPartitions);
      pendingLoop:
      while (!pending.isEmpty()) {
        OperatorMeta pp = pending.pop();
        for (StreamMeta s : pp.getInputStreams().values()) {
          if (currentMapping.parallelPartitions.contains(s.getSource().getOperatorWrapper()) && pending.contains(s.getSource().getOperatorWrapper())) {
            pending.push(pp);
            pending.push(s.getSource().getOperatorWrapper());
            continue pendingLoop;
          }
        }
        newOperators.add(addPTOperator(this.logicalToPTOperator.get(pp), null));
        // TODO: set checkpoint
      }

      // set checkpoint for new operator for deployment
      p.checkpointWindows.add(minCheckpoint);
      p.recoveryCheckpoint = minCheckpoint;
      try {
        ctx.getBackupAgent().backup(p.id, minCheckpoint, newPartition.getOperator(), StramUtils.getNodeSerDe(null));
      } catch (IOException e) {
        // inconsistent state, no recovery option, requires shutdown
        throw new IllegalStateException("Failed to write operator state after partition change " + p, e);
      }
    }

    // assign containers to new operators
    for (PTOperator oper : newOperators) {
      PTContainer newContainer = null;
      // check for existing inline set
      for (PTOperator inlineOper : oper.getGrouping(PTOperator.LocalityType.CONTAINER_LOCAL)) {
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
      PTContainer c = inlineContainer;
      if (c == null) {
        c = findContainer(oper);
        if (c == null) {
          // get new container
          LOG.debug("New container for partition: " + oper);
          c = new PTContainer(this);
          newContainers.add(c);
        }
      }
      oper.container = c;
      oper.container.operators.add(oper); // TODO: thread safety
    }

    deployOperators = this.getDependents(deployOperators);
    containers.addAll(newContainers);
    ctx.redeploy(undeployOperators, newContainers, deployOperators);

  }

  /**
   * Remove the given partition with any associated parallel partitions and
   * per-partition unifiers.
   *
   * @param oper
   * @return
   */
  private void removePartition(PTOperator oper, Set<DAG.OperatorMeta> parallelPartitions) {

    // remove any parallel partition
    for (PTOutput out : oper.outputs) {
      // copy list as it is modified by recursive remove
      for (PTInput in : Lists.newArrayList(out.sinks)) {
        for (DAG.InputPortMeta im : in.logicalStream.getSinks()) {
          PMapping m = this.logicalToPTOperator.get(im.getOperatorWrapper());
          if (m.parallelPartitions == parallelPartitions) {
            // associated operator parallel partitioned
            removePartition((PTOperator)in.target, parallelPartitions);
            m.partitions.remove(in.target);
          }
        }
      }
    }
    // remove the operator
    LOG.debug("Removing operator " + oper);
    removePTOperator(oper);
    // TODO: remove checkpoint states

    oper.container.operators.remove(oper); // TODO: thread safety

    // per partition merge operators
    if (!oper.upstreamMerge.isEmpty()) {
      for (Map.Entry<InputPortMeta, PTOperator> mEntry : oper.upstreamMerge.entrySet()) {
        removePTOperator(mEntry.getValue());
        mEntry.getValue().container.operators.remove(mEntry.getValue());
      }
    }
  }

  private PTContainer findContainer(PTOperator p) {
    // TODO: find container based on utilization
    return null;
  }

  private PTOperator addPTOperator(PMapping nodeDecl, Partition<?> partition) {
    PTOperator pOperator = createInstance(nodeDecl, partition);
    nodeDecl.addPartition(pOperator);

    Map<DAG.InputPortMeta, PartitionKeys> partitionKeys = Collections.emptyMap();
    if (partition != null) {
      partitionKeys = new HashMap<DAG.InputPortMeta, PartitionKeys>(partition.getPartitionKeys().size());
      Map<InputPort<?>, PartitionKeys> partKeys = partition.getPartitionKeys();
      for (Map.Entry<InputPort<?>, PartitionKeys> portEntry : partKeys.entrySet()) {
        DAG.InputPortMeta pportMeta = nodeDecl.logicalOperator.getMeta(portEntry.getKey());
        if (pportMeta == null) {
          throw new IllegalArgumentException("Invalid port reference " + portEntry);
        }
        partitionKeys.put(pportMeta, portEntry.getValue());
      }
    }

    for (Map.Entry<DAG.InputPortMeta, StreamMeta> inputEntry : nodeDecl.logicalOperator.getInputStreams().entrySet()) {
      // find upstream node(s), (can be multiple partitions)
      StreamMeta streamDecl = inputEntry.getValue();
      if (streamDecl.getSource() != null) {
        PMapping upstream = logicalToPTOperator.get(streamDecl.getSource().getOperatorWrapper());
        Collection<PTOperator> upstreamNodes = upstream.partitions;
        if (inputEntry.getKey().getAttributes().attrValue(PortContext.PARTITION_PARALLEL, false)) {
          if (upstream.partitions.size() < nodeDecl.partitions.size()) {
            throw new AssertionError("Number of partitions don't match in parallel mapping");
          }
          // pick upstream partition for new instance to attach to
          upstreamNodes = Collections.singletonList(upstream.partitions.get(nodeDecl.partitions.size()-1));
        }
        else if (upstream.partitions.size() > 1) {
          PTOperator mergeNode = upstream.mergeOperators.get(streamDecl.getSource());
          if (mergeNode == null) {
            // create the merge operator
            Unifier<?> unifier = streamDecl.getSource().getUnifier();
            if (unifier == null) {
              LOG.debug("Using default unifier for {}", streamDecl.getSource());
              unifier = new DefaultUnifier();
            }
            PortMappingDescriptor mergeDesc = new PortMappingDescriptor();
            Operators.describe(unifier, mergeDesc);
            if (mergeDesc.outputPorts.size() != 1) {
              throw new IllegalArgumentException("Merge operator should have single output port, found: " + mergeDesc.outputPorts);
            }
            mergeNode = new PTOperator(this, idSequence.incrementAndGet(), upstream.logicalOperator.getId() + "#merge#" + streamDecl.getSource().getPortName());
            mergeNode.logicalNode = upstream.logicalOperator;
            mergeNode.inputs = new ArrayList<PTInput>();
            mergeNode.outputs = new ArrayList<PTOutput>();
            mergeNode.merge = unifier;
            mergeNode.outputs.add(new PTOutput(this, mergeDesc.outputPorts.keySet().iterator().next(), streamDecl, mergeNode));

            PartitionKeys pks = partitionKeys.get(inputEntry.getKey());

            // add existing partitions as inputs
            for (PTOperator upstreamInstance : upstream.partitions) {
              for (PTOutput upstreamOut : upstreamInstance.outputs) {
                if (upstreamOut.logicalStream == streamDecl) {
                  // merge operator input
                  PTInput input = new PTInput("<merge#" + streamDecl.getSource().getPortName() + ">", streamDecl, mergeNode, pks, upstreamOut);
                  mergeNode.inputs.add(input);
                }
              }
            }

            if (pks == null) {
              upstream.mergeOperators.put(streamDecl.getSource(), mergeNode);
            } else {
              // NxM partitioning: create unifier per upstream partition
              LOG.debug("Partitioned unifier for {} {} {}", new Object[] {pOperator, inputEntry.getKey().getPortName(), pks});
              pOperator.upstreamMerge.put(inputEntry.getKey(), mergeNode);
            }

          }
          upstreamNodes = Collections.singletonList(mergeNode);
        }

        for (PTOperator upNode : upstreamNodes) {
          // link to upstream output(s) for this stream
          for (PTOutput upstreamOut : upNode.outputs) {
            if (upstreamOut.logicalStream == streamDecl) {
              PartitionKeys pks = partitionKeys.get(inputEntry.getKey());
              if (pOperator.upstreamMerge.containsKey(inputEntry.getKey())) {
                pks = null; // partitions applied to unifier input
              }
              PTInput input = new PTInput(inputEntry.getKey().getPortName(), streamDecl, pOperator, pks, upstreamOut);
              pOperator.inputs.add(input);
            }
          }
        }
      }
    }

    //
    // update locality
    //
    setLocalityGrouping(nodeDecl, pOperator, inlinePrefs, PTOperator.LocalityType.CONTAINER_LOCAL);
    setLocalityGrouping(nodeDecl, pOperator, localityPrefs, PTOperator.LocalityType.NODE_LOCAL);

    return pOperator;
  }

  private PTOperator createInstance(PMapping mapping, Partition<?> partition) {
    PTOperator pOperator = new PTOperator(this, idSequence.incrementAndGet(), mapping.logicalOperator.getId());
    pOperator.logicalNode = mapping.logicalOperator;
    pOperator.inputs = new ArrayList<PTInput>();
    pOperator.outputs = new ArrayList<PTOutput>();
    pOperator.partition = partition;

    // output port objects - these could be deferred until inputs are connected
    for (Map.Entry<DAG.OutputPortMeta, StreamMeta> outputEntry : mapping.logicalOperator.getOutputStreams().entrySet()) {
      PTOutput out = new PTOutput(this, outputEntry.getKey().getPortName(), outputEntry.getValue(), pOperator);
      pOperator.outputs.add(out);

      PTOperator merge = mapping.mergeOperators.get(outputEntry.getKey());
      if (merge != null) {
        // dynamically added partitions need to feed into existing unifier
        PTInput input = new PTInput("<merge#" + out.portName + ">", out.logicalStream, merge, null, out);
        merge.inputs.add(input);
      }
    }
    return pOperator;
  }

  private void setLocalityGrouping(PMapping pnodes, PTOperator newOperator, LocalityPrefs localityPrefs, PTOperator.LocalityType ltype) {

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

  private void removePTOperator(PTOperator node) {
    OperatorMeta nodeDecl = node.logicalNode;
    PMapping mapping = logicalToPTOperator.get(node.logicalNode);
    for (Map.Entry<DAG.OutputPortMeta, StreamMeta> outputEntry : nodeDecl.getOutputStreams().entrySet()) {
      PTOperator merge = mapping.mergeOperators.get(outputEntry.getKey());
      if (merge != null) {
        List<PTInput> newInputs = new ArrayList<PTInput>(merge.inputs.size());
        for (PTInput sinkIn : merge.inputs) {
          if (sinkIn.source.source != node) {
            newInputs.add(sinkIn);
          }
        }
        merge.inputs = newInputs;
      } else {
        StreamMeta streamDecl = outputEntry.getValue();
        for (DAG.InputPortMeta inp : streamDecl.getSinks()) {
          List<PTOperator> sinkNodes = logicalToPTOperator.get(inp.getOperatorWrapper()).partitions;
          for (PTOperator sinkNode : sinkNodes) {
            // unlink from downstream operators
            List<PTInput> newInputs = new ArrayList<PTInput>(sinkNode.inputs.size());
            for (PTInput sinkIn : sinkNode.inputs) {
              if (sinkIn.source.source != node) {
                newInputs.add(sinkIn);
              } else {
                sinkIn.source.sinks.remove(sinkIn);
              }
            }
            sinkNode.inputs = newInputs;
          }
        }
      }
    }
    // remove from upstream operators
    for (PTInput in : node.inputs) {
      in.source.sinks.remove(in);
    }
  }

  public DAG getDAG() {
    return this.dag;
  }

  protected List<PTContainer> getContainers() {
    return this.containers;
  }

  public List<PTOperator> getOperators(OperatorMeta logicalOperator) {
    return this.logicalToPTOperator.get(logicalOperator).partitions;
  }

  // used for recovery, this can go once plan traversal is fully encapsulated
  protected Map<DAG.OutputPortMeta, PTOperator> getMergeOperators(OperatorMeta logicalOperator) {
    return this.logicalToPTOperator.get(logicalOperator).mergeOperators;
  }

  protected List<OperatorMeta> getRootOperators() {
    return dag.getRootOperators();
  }

  protected List<PTOperator> getAllOperators() {
    List<PTOperator> list = new ArrayList<PTOperator>();
    for (Map.Entry<OperatorMeta, PMapping> entry : logicalToPTOperator.entrySet()) {
      list.addAll(entry.getValue().partitions);
    }
    return list;
  }

  private void getDeps(PTOperator operator, Set<PTOperator> visited) {
    visited.add(operator);
    for (PTInput in : operator.inputs) {
      if (in.source.isDownStreamInline()) {
        PTOperator sourceOperator = (PTOperator)in.source.source;
        if (!visited.contains(sourceOperator)) {
          getDeps(sourceOperator, visited);
        }
      }
    }
    // downstream traversal
    for (PTOutput out: operator.outputs) {
      for (PhysicalPlan.PTInput sink : out.sinks) {
        PTOperator sinkOperator = (PTOperator)sink.target;
        if (!visited.contains(sinkOperator)) {
          getDeps(sinkOperator, visited);
        }
      }
    }
  }

  /**
   * Get all operator instances that depend on the specified operator instance(s).
   * Dependencies are all downstream and upstream inline operators.
   * @param p
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

}

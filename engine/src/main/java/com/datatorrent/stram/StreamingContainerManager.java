/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.StramChildAgent.ContainerStartRequest;
import com.datatorrent.stram.api.ContainerContext;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat.DeployState;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.plan.logical.LogicalPlanRequest;
import com.datatorrent.stram.plan.logical.Operators;
import com.datatorrent.stram.plan.physical.OperatorStatus;
import com.datatorrent.stram.plan.physical.OperatorStatus.PortStatus;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PTOperator.PTInput;
import com.datatorrent.stram.plan.physical.PTOperator.PTOutput;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.plan.physical.PhysicalPlan.PlanContext;
import com.datatorrent.stram.plan.physical.PlanModifier;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient;
import com.datatorrent.stram.webapp.OperatorInfo;
import com.datatorrent.stram.webapp.PortInfo;
import com.datatorrent.stram.webapp.StreamInfo;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.bus.config.BusConfiguration;

/**
 *
 * Tracks topology provisioning/allocation to containers<p>
 * <br>
 * The tasks include<br>
 * Provisioning operators one container at a time. Each container gets assigned the operators, streams and its context<br>
 * Monitors run time operations including heartbeat protocol and node status<br>
 * Operator recovery and restart<br>
 * <br>
 *
 * @since 0.3.2
 */
public class StreamingContainerManager implements PlanContext
{
  private final static Logger LOG = LoggerFactory.getLogger(StreamingContainerManager.class);
  private final long windowStartMillis;
  private final int heartbeatTimeoutMillis;
  private final int maxWindowsBehindForStats;
  private int recordStatsInterval = 0;
  private long lastRecordStatsTime = 0;
  private SharedPubSubWebSocketClient wsClient;
  private FSStatsRecorder statsRecorder;
  private FSEventRecorder eventRecorder;
  private final String appPath;
  private final String checkpointFsPath;
  private String statsFsPath = null;
  private String eventsFsPath = null;
  protected final Map<String, String> containerStopRequests = new ConcurrentHashMap<String, String>();
  protected final ConcurrentLinkedQueue<ContainerStartRequest> containerStartRequests = new ConcurrentLinkedQueue<ContainerStartRequest>();
  protected final ConcurrentLinkedQueue<Runnable> eventQueue = new ConcurrentLinkedQueue<Runnable>();
  private final AtomicBoolean eventQueueProcessing = new AtomicBoolean();
  private final HashSet<PTContainer> pendingAllocation = Sets.newLinkedHashSet();
  protected String shutdownDiagnosticsMessage = "";
  protected boolean forcedShutdown = false;
  private long lastResourceRequest = 0;
  private final Map<String, StramChildAgent> containers = new ConcurrentHashMap<String, StramChildAgent>();
  private final PhysicalPlan plan;
  private final List<Pair<PTOperator, Long>> purgeCheckpoints = new ArrayList<Pair<PTOperator, Long>>();
  private final AlertsManager alertsManager = new AlertsManager(this);
  private CriticalPathInfo criticalPathInfo;

  private MBassador<StramEvent> eventBus; // event bus for publishing stram events

  // window id to node id to end window stats
  private final ConcurrentSkipListMap<Long, Map<Integer, EndWindowStats>> endWindowStatsOperatorMap = new ConcurrentSkipListMap<Long, Map<Integer, EndWindowStats>>();
  private long committedWindowId;

  // (operator id, port name) to timestamp
  private final Map<Pair<Integer, String>, Long> lastEndWindowTimestamps = new HashMap<Pair<Integer, String>, Long>();

  private static class EndWindowStats
  {
    long emitTimestamp = -1;
    HashMap<String, Long> dequeueTimestamps = new HashMap<String, Long>(); // input port name to end window dequeue time
  }

  public static class CriticalPathInfo
  {
    long latency;
    LinkedList<Integer> path = new LinkedList<Integer>();
  }

  public StreamingContainerManager(LogicalPlan dag)
  {
    this(dag, false);
  }

  public StreamingContainerManager(LogicalPlan dag, boolean enableEventRecording)
  {
    AttributeMap attributes = dag.getAttributes();

    if (attributes.get(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS) == null) {
      attributes.put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 500);
    }
    /* try to align to it to please eyes. */
    long tms = System.currentTimeMillis();
    windowStartMillis = tms - (tms % 1000);

    if (attributes.get(LogicalPlan.APPLICATION_PATH) == null) {
      attributes.put(LogicalPlan.APPLICATION_PATH, "stram/" + tms);
    }

    this.appPath = attributes.get(LogicalPlan.APPLICATION_PATH);
    this.checkpointFsPath = this.appPath + "/" + LogicalPlan.SUBDIR_CHECKPOINTS;
    this.statsFsPath = this.appPath + "/" + LogicalPlan.SUBDIR_STATS;

    if (enableEventRecording) {
      this.eventBus = new MBassador<StramEvent>(BusConfiguration.Default(1, 1, 1));
    }

    if (attributes.get(LogicalPlan.CHECKPOINT_WINDOW_COUNT) == null) {
      attributes.put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 30000 / attributes.get(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS));
    }
    this.heartbeatTimeoutMillis = dag.getValue(LogicalPlan.HEARTBEAT_TIMEOUT_MILLIS);

    if (attributes.get(LogicalPlan.STATS_MAX_ALLOWABLE_WINDOWS_LAG) == null) {
      attributes.put(LogicalPlan.STATS_MAX_ALLOWABLE_WINDOWS_LAG, 1000);
    }
    this.maxWindowsBehindForStats = attributes.get(LogicalPlan.STATS_MAX_ALLOWABLE_WINDOWS_LAG);

    if (attributes.get(LogicalPlan.STATS_RECORD_INTERVAL_MILLIS) == null) {
      attributes.put(LogicalPlan.STATS_RECORD_INTERVAL_MILLIS, 0);
    }
    this.recordStatsInterval = attributes.get(LogicalPlan.STATS_RECORD_INTERVAL_MILLIS);

    if (this.recordStatsInterval > 0 || enableEventRecording) {
      setupWsClient(attributes);
      if (this.recordStatsInterval > 0) {
        statsRecorder = new FSStatsRecorder();
        statsRecorder.setBasePath(this.statsFsPath);
        statsRecorder.setWebSocketClient(wsClient);
        statsRecorder.setup();
      }
      if (enableEventRecording) {
        this.eventsFsPath = this.appPath + "/" + LogicalPlan.SUBDIR_EVENTS;
        eventRecorder = new FSEventRecorder(attributes.get(LogicalPlan.APPLICATION_ID));
        eventRecorder.setBasePath(this.eventsFsPath);
        eventRecorder.setWebSocketClient(wsClient);
        eventRecorder.setup();
        eventBus.subscribe(eventRecorder);
      }
    }
    this.plan = new PhysicalPlan(dag, this);
  }

  private void setupWsClient(AttributeMap attributes)
  {
    String gatewayAddress = attributes.get(LogicalPlan.GATEWAY_ADDRESS);
    if (gatewayAddress != null) {
      try {
        wsClient = new SharedPubSubWebSocketClient("ws://" + gatewayAddress + "/pubsub", 500);
      }
      catch (Exception ex) {
        LOG.warn("Cannot establish websocket connection to {}", gatewayAddress);
      }
    }
  }

  public void teardown()
  {
    if (eventBus != null) {
      eventBus.shutdown();
    }
  }

  public void subscribeToEvents(Object listener)
  {
    if (eventBus != null) {
      eventBus.subscribe(listener);
    }
  }

  protected PhysicalPlan getPhysicalPlan()
  {
    return plan;
  }

  public long getCommittedWindowId()
  {
    return committedWindowId;
  }

  /**
   * Check periodically that deployed containers phone home.
   * Run from the master main loop (single threaded access).
   */
  public void monitorHeartbeat()
  {
    long currentTms = System.currentTimeMillis();

    // look for resource allocation timeout
    if (!pendingAllocation.isEmpty()) {
      // look for resource allocation timeout
      if (lastResourceRequest + plan.getDAG().getValue(LogicalPlan.RESOURCE_ALLOCATION_TIMEOUT_MILLIS) < currentTms) {
        String msg = String.format("Shutdown due to resource allocation timeout (%s ms) waiting for %s containers", currentTms - lastResourceRequest, pendingAllocation.size());
        LOG.warn(msg);
        for (PTContainer c : pendingAllocation) {
          LOG.warn("Waiting for resource: {}m priority: {} {}", c.getRequiredMemoryMB(), c.getResourceRequestPriority(), c);
        }
        forcedShutdown = true;
        shutdownAllContainers(msg);
      }
      else {
        for (PTContainer c : pendingAllocation) {
          LOG.debug("Waiting for resource: {}m {}", c.getRequiredMemoryMB(), c);
        }
      }
    }

    boolean pendingDeployChanges = false;

    // monitor currently deployed containers
    for (StramChildAgent sca : containers.values()) {
      PTContainer c = sca.container;
      pendingDeployChanges |= !(c.getPendingDeploy().isEmpty() && c.getPendingUndeploy().isEmpty());

      if (!pendingAllocation.contains(c) && c.getExternalId() != null) {
        if (sca.lastHeartbeatMillis == 0) {
          // container allocated but process was either not launched or is not able to phone home
          if (currentTms - sca.createdMillis > 2 * heartbeatTimeoutMillis) {
            LOG.info("Container {}@{} startup timeout ({} ms).", new Object[] {c.getExternalId(), c.host, currentTms - sca.createdMillis});
            containerStopRequests.put(c.getExternalId(), c.getExternalId());
          }
        } else {
          if (currentTms - sca.lastHeartbeatMillis > heartbeatTimeoutMillis) {
            if (!isApplicationIdle()) {
              // request stop (kill) as process may still be hanging around (would have been detected by Yarn otherwise)
              LOG.info("Container {}@{} heartbeat timeout ({} ms).", new Object[] {c.getExternalId(), c.host, currentTms - sca.lastHeartbeatMillis});
              containerStopRequests.put(c.getExternalId(), c.getExternalId());
            }
          }
        }
      }
    }

    if (!pendingDeployChanges) {
      // events that may modify the plan
      processEvents();
    }

    committedWindowId = updateCheckpoints();
    calculateEndWindowStats();
    if (recordStatsInterval > 0 && (lastRecordStatsTime + recordStatsInterval <= currentTms)) {
      recordStats(currentTms);
    }
  }

  private void recordStats(long currentTms)
  {
    try {
      statsRecorder.recordContainers(containers, currentTms);
      statsRecorder.recordOperators(getOperatorInfoList(), currentTms);
      lastRecordStatsTime = System.currentTimeMillis();
    } catch (Exception ex) {
      LOG.warn("Exception caught when recording stats", ex);
    }
  }

  private void calculateEndWindowStats()
  {
    if (!endWindowStatsOperatorMap.isEmpty()) {
      if (endWindowStatsOperatorMap.size() > maxWindowsBehindForStats) {
        LOG.warn("Some operators are behind for more than {} windows! Trimming the end window stats map", maxWindowsBehindForStats);
        while (endWindowStatsOperatorMap.size() > maxWindowsBehindForStats) {
          endWindowStatsOperatorMap.remove(endWindowStatsOperatorMap.firstKey());
        }
      }

      Set<Integer> allCurrentOperators = plan.getAllOperators().keySet();
      int numOperators = allCurrentOperators.size();
      Long windowId = endWindowStatsOperatorMap.firstKey();
      while (windowId != null) {
        Map<Integer, EndWindowStats> endWindowStatsMap = endWindowStatsOperatorMap.get(windowId);
        Set<Integer> endWindowStatsOperators = endWindowStatsMap.keySet();

        if (allCurrentOperators.containsAll(endWindowStatsOperators)) {
          if (endWindowStatsMap.size() < numOperators) {
            break;
          }
          else {
            // collected data from all operators for this window id.  start latency calculation
            List<OperatorMeta> rootOperatorMetas = plan.getDAG().getRootOperators();
            Set<PTOperator> endWindowStatsVisited = new HashSet<PTOperator>();
            Set<PTOperator> leafOperators = new HashSet<PTOperator>();
            for (OperatorMeta root: rootOperatorMetas) {
              List<PTOperator> rootOperators = plan.getOperators(root);
              for (PTOperator rootOperator: rootOperators) {
                // DFS for visiting the operators for latency calculation
                calculateLatency(rootOperator, endWindowStatsMap, endWindowStatsVisited, leafOperators);
              }
            }
            CriticalPathInfo cpi = new CriticalPathInfo();
            //LOG.debug("Finding critical path...");
            cpi.latency = findCriticalPath(endWindowStatsMap, leafOperators, cpi.path);
            criticalPathInfo = cpi;
            endWindowStatsOperatorMap.remove(windowId);
          }
        }
        else {
          // the old stats contains operators that do not exist any more
          // this is probably right after a partition happens.
          endWindowStatsOperatorMap.remove(windowId);
        }
        windowId = endWindowStatsOperatorMap.higherKey(windowId);
      }
    }
  }

  private void calculateLatency(PTOperator oper, Map<Integer, EndWindowStats> endWindowStatsMap, Set<PTOperator> endWindowStatsVisited, Set<PTOperator> leafOperators)
  {
    endWindowStatsVisited.add(oper);
    OperatorStatus operatorStatus = oper.stats;
    if (operatorStatus == null) {
      LOG.info("Operator status for operator {} does not exist yet.", oper);
      return;
    }

    EndWindowStats endWindowStats = endWindowStatsMap.get(oper.getId());
    if (endWindowStats == null) {
      LOG.info("End window stats is null for operator {}, probably a new operator after partitioning", oper);
      return;
    }

    // find the maximum end window emit time from all input ports
    long upstreamMaxEmitTimestamp = -1;
    for (PTOperator.PTInput input: oper.getInputs()) {
      if (input.source.source instanceof PTOperator) {
        PTOperator upstreamOp = input.source.source;
        EndWindowStats upstreamEndWindowStats = endWindowStatsMap.get(upstreamOp.getId());
        if (upstreamEndWindowStats == null) {
          LOG.info("End window stats is null for operator {}", oper);
          return;
        }
        if (upstreamEndWindowStats.emitTimestamp > upstreamMaxEmitTimestamp) {
          upstreamMaxEmitTimestamp = upstreamEndWindowStats.emitTimestamp;
        }
      }
    }

    if (upstreamMaxEmitTimestamp > 0) {
      if (upstreamMaxEmitTimestamp < endWindowStats.emitTimestamp) {
        //LOG.debug("Adding {} to latency MA for {}", endWindowStats.emitTimestamp - upstreamMaxEmitTimestamp, oper);
        operatorStatus.latencyMA.add(endWindowStats.emitTimestamp - upstreamMaxEmitTimestamp);
      }
      else if (upstreamMaxEmitTimestamp != endWindowStats.emitTimestamp) {
        LOG.warn("Cannot add to latency MA because upstreamMaxEmitTimestamp is greater than emitTimestamp ({} > {})", upstreamMaxEmitTimestamp, endWindowStats.emitTimestamp);
        LOG.warn("for operator {}. Please verify that the system clocks are in sync in your cluster.", oper);
      }
    }

    if (oper.getOutputs().isEmpty()) {
      // it is a leaf operator
      leafOperators.add(oper);
    }
    else {
      for (PTOperator.PTOutput output : oper.getOutputs()) {
        for (PTOperator.PTInput input : output.sinks) {
          if (input.target instanceof PTOperator) {
            PTOperator downStreamOp = input.target;
            if (!endWindowStatsVisited.contains(downStreamOp)) {
              calculateLatency(downStreamOp, endWindowStatsMap, endWindowStatsVisited, leafOperators);
            }
          }
        }
      }
    }
  }

  /*
   * returns cumulative latency
   */
  private long findCriticalPath(Map<Integer, EndWindowStats> endWindowStatsMap, Set<PTOperator> operators, LinkedList<Integer> criticalPath)
  {
    long maxEndWindowTimestamp = 0;
    PTOperator maxOperator = null;
    for (PTOperator operator : operators) {
      EndWindowStats endWindowStats = endWindowStatsMap.get(operator.getId());
      if (maxEndWindowTimestamp < endWindowStats.emitTimestamp) {
        maxEndWindowTimestamp = endWindowStats.emitTimestamp;
        maxOperator = operator;
      }
    }
    if (maxOperator == null) {
      return 0;
    }
    criticalPath.addFirst(maxOperator.getId());
    OperatorStatus operatorStatus = maxOperator.stats;
    if (operatorStatus == null) {
      return 0;
    }
    operators.clear();
    if (maxOperator.getInputs() == null || maxOperator.getInputs().isEmpty()) {
      return operatorStatus.latencyMA.getAvg();
    }
    for (PTOperator.PTInput input : maxOperator.getInputs()) {
      if (input.source.source instanceof PTOperator) {
        operators.add(input.source.source);
      }
    }
    return operatorStatus.latencyMA.getAvg() + findCriticalPath(endWindowStatsMap, operators, criticalPath);
  }

  public int processEvents()
  {
    for (PTOperator o: plan.getAllOperators().values()) {
      List<OperatorStats> stats;
      if ((stats = o.stats.statsListenerReport.get()) != null) {
        plan.onStatusUpdate(o);
        o.stats.statsListenerReport.compareAndSet(stats, null);
      }
    }
    int count = 0;
    Runnable command;
    while ((command = this.eventQueue.poll()) != null) {
      eventQueueProcessing.set(true);
      try {
        command.run();
        count++;
      }
      catch (Exception e) {
        // TODO: handle error
        LOG.error("Failed to execute " + command, e);
      }
      eventQueueProcessing.set(false);
    }
    return count;
  }

  /**
   * Schedule container restart. Called by Stram after a container was terminated
   * and requires recovery (killed externally, or after heartbeat timeout). <br>
   * Recovery will resolve affected operators (within the container and
   * everything downstream with respective recovery checkpoint states).
   * Dependent operators will be undeployed and buffer server connections reset prior to
   * redeploy to recovery checkpoint.
   *
   * @param containerId
   */
  public void scheduleContainerRestart(String containerId)
  {

    StramChildAgent cs = getContainerAgent(containerId);
    if (cs.shutdownRequested == true) {
      return;
    }

    LOG.info("Initiating recovery for {}@{}", containerId, cs.container.host);

    cs.container.setState(PTContainer.State.KILLED);
    cs.container.bufferServerAddress = null;

    // building the checkpoint dependency,
    // downstream operators will appear first in map
    LinkedHashSet<PTOperator> checkpoints = new LinkedHashSet<PTOperator>();

    MutableLong ml = new MutableLong();
    for (PTOperator oper: cs.container.getOperators()) {
      // TODO: traverse container local upstream operators
      updateRecoveryCheckpoints(oper, checkpoints, ml);
    }

    // redeploy cycle for all affected operators
    LOG.info("Affected operators {}", checkpoints);
    deploy(Collections.<PTContainer>emptySet(), checkpoints, Sets.newHashSet(cs.container), checkpoints);
  }

  public void removeContainerAgent(String containerId)
  {
    LOG.debug("Removing container agent {}", containerId);
    containers.remove(containerId);
  }

  public static class ContainerResource
  {
    public final String containerId;
    public final String host;
    public final int memoryMB;
    public final int priority;
    public final String nodeHttpAddress;

    public ContainerResource(int priority, String containerId, String host, int memoryMB, String nodeHttpAddress)
    {
      this.containerId = containerId;
      this.host = host;
      this.memoryMB = memoryMB;
      this.priority = priority;
      this.nodeHttpAddress = nodeHttpAddress;
    }

    /**
     *
     * @return String
     */
    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
              .append("containerId", this.containerId)
              .append("host", this.host)
              .append("memoryMB", this.memoryMB)
              .toString();
    }

  }

  /**
   * Assign operators to allocated container resource.
   *
   * @param resource
   * @param bufferServerAddr
   * @return
   */
  public StramChildAgent assignContainer(ContainerResource resource, InetSocketAddress bufferServerAddr)
  {
    PTContainer container = null;
    // match container waiting for resource
    for (PTContainer c : pendingAllocation) {
      if (c.getState() == PTContainer.State.NEW || c.getState() == PTContainer.State.KILLED) {
        if (c.getResourceRequestPriority() == resource.priority) {
          container = c;
        }
      }
    }

    if (container == null) {
      LOG.debug("No container matching allocated resource {}", resource);
      LOG.debug("Containers waiting for allocation {}", pendingAllocation);
      return null;
    }

    pendingAllocation.remove(container);
    container.setState(PTContainer.State.ALLOCATED);
    if (container.getExternalId() != null) {
      LOG.info("Replacing container agent {}", container.getExternalId());
      this.containers.remove(container.getExternalId());
    }
    container.setExternalId(resource.containerId);
    container.host = resource.host;
    container.bufferServerAddress = bufferServerAddr;
    container.nodeHttpAddress = resource.nodeHttpAddress;
    container.setAllocatedMemoryMB(resource.memoryMB);
    container.getPendingUndeploy().clear();

    StramChildAgent sca = new StramChildAgent(container, newStreamingContainerContext(resource.containerId), this);
    containers.put(resource.containerId, sca);
    return sca;
  }

  private StreamingContainerContext newStreamingContainerContext(String containerId)
  {
    StreamingContainerContext scc = new StreamingContainerContext(new DefaultAttributeMap(), plan.getDAG());
    scc.attributes.put(ContainerContext.IDENTIFIER, containerId);
    scc.startWindowMillis = this.windowStartMillis;
    return scc;
  }

  public StramChildAgent getContainerAgent(String containerId)
  {
    StramChildAgent cs = containers.get(containerId);
    if (cs == null) {
      throw new AssertionError("Unknown container " + containerId);
    }
    return cs;
  }

  public Collection<StramChildAgent> getContainerAgents()
  {
    return this.containers.values();
  }

  private PTOperator updateOperatorStatus(OperatorHeartbeat shb) {
    PTOperator oper = this.plan.getAllOperators().get(shb.getNodeId());
    if (oper != null) {
      PTContainer container = oper.getContainer();
      if (oper.getState() == PTOperator.State.PENDING_DEPLOY) {
        // remove operator from deploy list unless it was again scheduled for undeploy (or redeploy)
        if (!container.getPendingUndeploy().contains(oper)) {
           container.getPendingDeploy().remove(oper);
        }

        LOG.debug("{} marking deployed: {} remote status {} pendingDeploy {}", new Object[] {container.getExternalId(), oper, shb.getState(), container.getPendingDeploy()});
        oper.setState(PTOperator.State.ACTIVE);
        oper.stats.lastHeartbeat = null; // reset on redeploy

        recordEventAsync(new StramEvent.StartOperatorEvent(oper.getName(), oper.getId(), container.getExternalId()));
      }
    }
    return oper;
  }

  /**
   * process the heartbeat from each container.
   * called by the RPC thread for each container. (i.e. called by multiple threads)
   *
   * @param heartbeat
   * @return
   */
  public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat heartbeat)
  {
    long currentTimeMillis = System.currentTimeMillis();

    StramChildAgent sca = this.containers.get(heartbeat.getContainerId());
    if (sca == null || sca.container.getState() == PTContainer.State.KILLED) {
      // could be orphaned container that was replaced and needs to terminate
      LOG.error("Unknown container " + heartbeat.getContainerId());
      ContainerHeartbeatResponse response = new ContainerHeartbeatResponse();
      response.shutdown = true;
      return response;
    }

    //LOG.debug("{} {} {}", new Object[]{sca.container.containerId, sca.container.bufferServerAddress, sca.container.getState()});
    if (sca.container.getState() == PTContainer.State.ALLOCATED) {
      // capture dynamically assigned address from container
      if (sca.container.bufferServerAddress == null && heartbeat.bufferServerHost != null) {
        sca.container.bufferServerAddress = InetSocketAddress.createUnresolved(heartbeat.bufferServerHost, heartbeat.bufferServerPort);
        LOG.info("Container {} buffer server: {}", sca.container.getExternalId(), sca.container.bufferServerAddress);
      }
      sca.container.setState(PTContainer.State.ACTIVE);
      sca.jvmName = heartbeat.jvmName;
    }

    if (heartbeat.restartRequested) {
      LOG.error("Container {} restart request", sca.container.getExternalId());
      containerStopRequests.put(sca.container.getExternalId(), sca.container.getExternalId());
    }

    // commented out because free memory is misleading because of GC. may want to revisit this.
    //sca.memoryMBFree = heartbeat.memoryMBFree;

    boolean containerIdle = true;
    long elapsedMillis = 0;

    for (OperatorHeartbeat shb: heartbeat.getContainerStats().operators) {

      PTOperator oper = updateOperatorStatus(shb);
      if (oper == null) {
        LOG.error("Heartbeat for unknown operator {} (container {})", shb.getNodeId(), heartbeat.getContainerId());
        continue;
      }

      oper.stats.recordingStartTime = Stats.INVALID_TIME_MILLIS;

      //LOG.debug("heartbeat {} {}/{} {}", oper, oper.getState(), shb.getState(), oper.getContainer().getExternalId());

      OperatorHeartbeat previousHeartbeat = oper.stats.lastHeartbeat;
      oper.stats.lastHeartbeat = shb;

      if (shb.getState().compareTo(DeployState.FAILED.name()) == 0) {
        // count failure transitions *->FAILED, applies to initialization as well as intermittent failures
        if (previousHeartbeat == null || DeployState.FAILED.name().compareTo(previousHeartbeat.getState()) != 0) {
          oper.failureCount++;
          LOG.warn("Operator failure: {} count: {}", oper, oper.failureCount);
          Integer maxAttempts = oper.getOperatorMeta().getValue(OperatorContext.RECOVERY_ATTEMPTS);
          if (oper.failureCount <= maxAttempts) {
            // restart entire container in attempt to recover operator
            // in the future a more sophisticated recovery strategy could
            // involve initial redeploy attempt(s) of affected operator in
            // existing container or sandbox container for just the operator
            LOG.error("Issuing container stop to restart after operator failure {}", oper);
            containerStopRequests.put(sca.container.getExternalId(), sca.container.getExternalId());
          }
          else {
            String msg = String.format("Shutdown after reaching failure threshold for %s", oper);
            LOG.warn(msg);
            forcedShutdown = true;
            shutdownAllContainers(msg);
          }
        }
      }

      if (!oper.stats.isIdle()) {
        containerIdle = false;

        long tuplesProcessed = 0;
        long tuplesEmitted = 0;
        long totalCpuTimeUsed = 0;
        long totalElapsedMillis = 0;
        int statCount=0;
        long maxDequeueTimestamp = -1;
        List<ContainerStats.OperatorStats> statsList = shb.getOperatorStatsContainer();

        final OperatorStatus status = oper.stats;
        for (ContainerStats.OperatorStats stats: statsList) {
          /* report checkpointedWindowId status of the operator */
          if (oper.getRecentCheckpoint() < stats.checkpointedWindowId) {
            addCheckpoint(oper, stats.checkpointedWindowId);
          }

          if (stats.recordingStartTime > oper.stats.recordingStartTime) {
            oper.stats.recordingStartTime = stats.recordingStartTime;
          }

          /* report all the other stuff */

          // calculate the stats related to end window
          EndWindowStats endWindowStats = new EndWindowStats(); // end window stats for a particular window id for a particular node
          Collection<ContainerStats.OperatorStats.PortStats> ports = stats.inputPorts;
          if (ports != null) {
            for (ContainerStats.OperatorStats.PortStats s : ports) {
              PortStatus ps = status.inputPortStatusList.get(s.id);
              if (ps == null) {
                ps = status.new PortStatus();
                ps.portName = s.id;
                status.inputPortStatusList.put(s.id, ps);
              }
              ps.totalTuples += s.tupleCount;

              if (s.recordingStartTime > ps.recordingStartTime) {
                ps.recordingStartTime = s.recordingStartTime;
              }

              tuplesProcessed += s.tupleCount;
              endWindowStats.dequeueTimestamps.put(s.id, s.endWindowTimestamp);

              Pair<Integer, String> operatorPortName = new Pair<Integer, String>(oper.getId(), s.id);
              if (lastEndWindowTimestamps.containsKey(operatorPortName) && (s.endWindowTimestamp > lastEndWindowTimestamps.get(operatorPortName))) {
                ps.tuplesPSMA.add(s.tupleCount, s.endWindowTimestamp - lastEndWindowTimestamps.get(operatorPortName));
                elapsedMillis = s.endWindowTimestamp - lastEndWindowTimestamps.get(operatorPortName);
              }else{
                elapsedMillis = s.endWindowTimestamp;
              }

              lastEndWindowTimestamps.put(operatorPortName, s.endWindowTimestamp);

              if (s.endWindowTimestamp > maxDequeueTimestamp) {
                maxDequeueTimestamp = s.endWindowTimestamp;
              }

              if (elapsedMillis > 0) {
                Long numBytes = s.bufferServerBytes;
                ps.bufferServerBytesPSMA.add(numBytes, elapsedMillis);
              }
            }
          }

          ports = stats.outputPorts;
          if (ports != null) {
            for (ContainerStats.OperatorStats.PortStats s: ports) {
              PortStatus ps = status.outputPortStatusList.get(s.id);
              if (ps == null) {
                ps = status.new PortStatus();
                ps.portName = s.id;
                status.outputPortStatusList.put(s.id, ps);
              }
              ps.totalTuples += s.tupleCount;

              if (s.recordingStartTime > ps.recordingStartTime) {
                ps.recordingStartTime = s.recordingStartTime;
              }

              tuplesEmitted += s.tupleCount;
              Pair<Integer, String> operatorPortName = new Pair<Integer, String>(oper.getId(), s.id);

              // the second condition is needed when
              // 1) the operator is redeployed and is playing back the tuples, or
              // 2) the operator is catching up very fast and the endWindowTimestamp of subsequent windows is less than one millisecond
              if (lastEndWindowTimestamps.containsKey(operatorPortName) &&
                      (s.endWindowTimestamp > lastEndWindowTimestamps.get(operatorPortName))) {
                ps.tuplesPSMA.add(s.tupleCount, s.endWindowTimestamp - lastEndWindowTimestamps.get(operatorPortName));
                elapsedMillis = s.endWindowTimestamp - lastEndWindowTimestamps.get(operatorPortName);
              }else{
                elapsedMillis = s.endWindowTimestamp;
              }

              lastEndWindowTimestamps.put(operatorPortName, s.endWindowTimestamp);

              if (elapsedMillis > 0) {
                Long numBytes = s.bufferServerBytes;
                ps.bufferServerBytesPSMA.add(numBytes, elapsedMillis);
              }
            }
            if (ports.size() > 0) {
              endWindowStats.emitTimestamp = ports.iterator().next().endWindowTimestamp;
            }
          }

          // for output operator, just take the maximum dequeue time for emit timestamp.
          // (we don't know the latency for output operators because they don't emit tuples)
          if (endWindowStats.emitTimestamp < 0) {
            endWindowStats.emitTimestamp = maxDequeueTimestamp;
          }

          status.currentWindowId = stats.windowId;
          totalCpuTimeUsed += stats.cpuTimeUsed;
          totalElapsedMillis += elapsedMillis;
          statCount++;

          Map<Integer, EndWindowStats> endWindowStatsMap = endWindowStatsOperatorMap.get(stats.windowId);
          if (endWindowStatsMap == null) {
            endWindowStatsOperatorMap.putIfAbsent(stats.windowId, new ConcurrentHashMap<Integer, EndWindowStats>());
            endWindowStatsMap = endWindowStatsOperatorMap.get(stats.windowId);
          }
          endWindowStatsMap.put(shb.getNodeId(), endWindowStats);
        }

        status.totalTuplesProcessed += tuplesProcessed;
        status.totalTuplesEmitted += tuplesEmitted;
        if (elapsedMillis > 0) {
          status.tuplesProcessedPSMA = 0;
          status.tuplesEmittedPSMA = 0;
          if(statCount != 0){
            status.cpuPercentageMA.add((double)totalCpuTimeUsed  / (totalElapsedMillis * 10000));
          }else{
            status.cpuPercentageMA.add(0.0);
          }
          for (PortStatus ps: status.inputPortStatusList.values()) {
            status.tuplesProcessedPSMA += ps.tuplesPSMA.getAvg();
          }
          for (PortStatus ps: status.outputPortStatusList.values()) {
            status.tuplesEmittedPSMA += ps.tuplesPSMA.getAvg();
          }

          status.lastWindowedStats = statsList;
          if (oper.statsListeners != null) {
            status.statsListenerReport.set(statsList);
          }
        }
      }
    }

    sca.lastHeartbeatMillis = currentTimeMillis;

    ContainerHeartbeatResponse rsp = sca.pollRequest();
    if (rsp == null) {
      rsp = new ContainerHeartbeatResponse();
    }

    // below should be merged into pollRequest
    if (containerIdle && isApplicationIdle()) {
      LOG.info("requesting idle shutdown for container {}", heartbeat.getContainerId());
      rsp.shutdown = true;
    }
    else {
      if (sca.shutdownRequested) {
        LOG.info("requesting shutdown for container {}", heartbeat.getContainerId());
        rsp.shutdown = true;
      }
    }

    List<StramToNodeRequest> requests = rsp.nodeRequests != null ? rsp.nodeRequests : new ArrayList<StramToNodeRequest>();
    ConcurrentLinkedQueue<StramToNodeRequest> operatorRequests = sca.getOperatorRequests();
    while (true) {
      StramToNodeRequest r = operatorRequests.poll();
      if (r == null) {
        break;
      }
      requests.add(r);
    }
    rsp.nodeRequests = requests;
    rsp.committedWindowId = committedWindowId;
    return rsp;
  }

  private boolean isApplicationIdle()
  {
    if (eventQueueProcessing.get()) {
      return false;
    }
    for (StramChildAgent sca: this.containers.values()) {
      if (sca.hasPendingWork()) {
        // container may have no active operators but deploy request pending
        return false;
      }
      for (PTOperator oper : sca.container.getOperators()) {
        if (!oper.stats.isIdle()) {
          return false;
        }
      }
    }
    return true;
  }

  void addCheckpoint(PTOperator node, long backupWindowId)
  {
    synchronized (node.checkpointWindows) {
      if (!node.checkpointWindows.isEmpty()) {
        Long lastCheckpoint = node.checkpointWindows.getLast();
        // skip unless checkpoint moves
        if (lastCheckpoint.longValue() != backupWindowId) {
          if (lastCheckpoint.longValue() > backupWindowId) {
            // list needs to have max windowId last
            LOG.warn("Out of sequence checkpoint {} last {} (operator {})", new Object[] {backupWindowId, lastCheckpoint, node});
            ListIterator<Long> li = node.checkpointWindows.listIterator();
            while (li.hasNext() && li.next().longValue() < backupWindowId) {
              continue;
            }
            if (li.previous() != backupWindowId) {
              li.add(backupWindowId);
            }
          }
          else {
            node.checkpointWindows.add(backupWindowId);
          }
        }
      }
      else {
        node.checkpointWindows.add(backupWindowId);
      }
    }
  }

  /**
   * Compute checkpoints required for a given operator instance to be recovered.
   * This is done by looking at checkpoints available for downstream dependencies first,
   * and then selecting the most recent available checkpoint that is smaller than downstream.
   *
   * @param operator Operator instance for which to find recovery checkpoint
   * @param visited Set into which to collect visited dependencies
   * @param committedWindowId
   */
  public void updateRecoveryCheckpoints(PTOperator operator, Set<PTOperator> visited, MutableLong committedWindowId)
  {
    if (operator.getRecoveryCheckpoint() < committedWindowId.longValue()) {
      committedWindowId.setValue(operator.getRecoveryCheckpoint());
    }

    long maxCheckpoint = operator.getRecentCheckpoint();
    // find smallest of most recent subscriber checkpoints
    for (PTOperator.PTOutput out: operator.getOutputs()) {
      for (PTOperator.PTInput sink: out.sinks) {
        PTOperator sinkOperator = sink.target;
        if (!visited.contains(sinkOperator)) {
          // downstream traversal
          updateRecoveryCheckpoints(sinkOperator, visited, committedWindowId);
        }
        // recovery window id cannot move backwards
        // when dynamically adding new operators
        if (sinkOperator.getRecoveryCheckpoint() >= operator.getRecoveryCheckpoint()) {
          maxCheckpoint = Math.min(maxCheckpoint, sinkOperator.getRecoveryCheckpoint());
        }
      }
    }

    // checkpoint frozen during deployment
    if (operator.getState() != PTOperator.State.PENDING_DEPLOY) {
      // remove previous checkpoints
      long c1 = OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID;
      synchronized (operator.checkpointWindows) {
        if (!operator.checkpointWindows.isEmpty()) {
          if ((c1 = operator.checkpointWindows.getFirst().longValue()) <= maxCheckpoint) {
            long c2 = OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID;
            while (operator.checkpointWindows.size() > 1 && (c2 = operator.checkpointWindows.get(1).longValue()) <= maxCheckpoint) {
              operator.checkpointWindows.removeFirst();
              //LOG.debug("Checkpoint to delete: operator={} windowId={}", operator.getName(), c1);
              this.purgeCheckpoints.add(new Pair<PTOperator, Long>(operator, c1));
              c1 = c2;
            }
          }
          else {
            c1 = OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID;
          }
        }
      }
      //LOG.debug("Operator {} checkpoints: commit {} recent {}", new Object[] {operator.getName(), c1, operator.checkpointWindows});
      operator.setRecoveryCheckpoint(c1);
    } else {
      LOG.debug("Skipping checkpoint update {} {}", operator, operator.getState());
    }

    visited.add(operator);
  }

  /**
   * Visit all operators to update current checkpoint based on updated downstream state.
   * Purge older checkpoints that are no longer needed.
   */
  private long updateCheckpoints()
  {
    MutableLong lCommittedWindowId = new MutableLong(Long.MAX_VALUE);

    Set<PTOperator> visitedCheckpoints = new LinkedHashSet<PTOperator>();
    for (OperatorMeta logicalOperator: plan.getDAG().getRootOperators()) {
      //LOG.debug("Updating checkpoints for operator {}", logicalOperator.getName());
      List<PTOperator> operators = plan.getOperators(logicalOperator);
      if (operators != null) {
        for (PTOperator operator: operators) {
          updateRecoveryCheckpoints(operator, visitedCheckpoints, lCommittedWindowId);
        }
      }
    }
    purgeCheckpoints();

    return lCommittedWindowId.longValue();
  }

  private BufferServerController getBufferServerClient(PTOperator operator)
  {
    BufferServerController bsc = new BufferServerController(operator.getLogicalId());
    InetSocketAddress address = operator.getContainer().bufferServerAddress;
    StramChild.eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsc);
    return bsc;
  }

  private void purgeCheckpoints()
  {
    StorageAgent ba = new FSStorageAgent(new Configuration(), checkpointFsPath);
    for (Pair<PTOperator, Long> p: purgeCheckpoints) {
      PTOperator operator = p.getFirst();
      try {
        ba.delete(operator.getId(), p.getSecond());
        //LOG.debug("Purged checkpoint {} {}", operator.getId(), p.getSecond());
      }
      catch (Exception e) {
        LOG.error("Failed to purge checkpoint " + p, e);
      }
      // delete stream state when using buffer server
      for (PTOperator.PTOutput out: operator.getOutputs()) {
        if (!out.isDownStreamInline()) {
          if (operator.getContainer().bufferServerAddress == null) {
            // address should be null only for a new container, in which case there should not be a purge request
            // TODO: logging added to find out how we got here
            LOG.warn("purge request w/o buffer server address source {} container {} checkpoints {}",
                new Object[] { out, operator.getContainer(), operator.checkpointWindows });
            continue;
          }
          // following needs to match the concat logic in StramChild
          String sourceIdentifier = Integer.toString(operator.getId()).concat(Component.CONCAT_SEPARATOR).concat(out.portName);
          // delete everything from buffer server prior to new checkpoint
          BufferServerController bsc = getBufferServerClient(operator);
          try {
            bsc.purge(null, sourceIdentifier, operator.checkpointWindows.getFirst() - 1);
          }
          catch (Throwable t) {
            LOG.error("Failed to purge " + bsc.addr + " " + sourceIdentifier, t);
          }
        }
      }
    }
    purgeCheckpoints.clear();
  }

  /**
   * Mark all containers for shutdown, next container heartbeat response
   * will propagate the shutdown request. This is controlled soft shutdown.
   * If containers don't respond, the application can be forcefully terminated
   * via yarn using forceKillApplication.
   *
   * @param message
   */
  public void shutdownAllContainers(String message)
  {
    this.shutdownDiagnosticsMessage = message;
    LOG.info("Initiating application shutdown: " + message);
    for (StramChildAgent cs: this.containers.values()) {
      cs.shutdownRequested = true;
    }
  }

  @Override
  public StorageAgent getStorageAgent()
  {
    return new FSStorageAgent(new Configuration(), this.checkpointFsPath);
  }

  private Map<PTContainer, List<PTOperator>> groupByContainer(Collection<PTOperator> operators)
  {
    Map<PTContainer, List<PTOperator>> m = new HashMap<PTContainer, List<PTOperator>>();
    for (PTOperator node: operators) {
      List<PTOperator> nodes = m.get(node.getContainer());
      if (nodes == null) {
        nodes = new ArrayList<PTOperator>();
        m.put(node.getContainer(), nodes);
      }
      nodes.add(node);
    }
    return m;
  }

  @Override
  public void deploy(Set<PTContainer> releaseContainers, Collection<PTOperator> undeploy, Set<PTContainer> startContainers, Collection<PTOperator> deploy)
  {

    Map<PTContainer, List<PTOperator>> undeployGroups = groupByContainer(undeploy);

    // stop affected operators (exclude new/failed containers)
    // order does not matter, remove all operators in each container in one sweep
    for (Map.Entry<PTContainer, List<PTOperator>> e: undeployGroups.entrySet()) {
      // container may already be in failed or pending deploy state, notified by RM or timed out
      PTContainer c = e.getKey();
      if (!startContainers.contains(c) && !releaseContainers.contains(c) && c.getState() != PTContainer.State.KILLED) {
        LOG.debug("scheduling undeploy {} {}", e.getKey().getExternalId(), e.getValue());
        for (PTOperator oper : e.getValue()) {
          c.getPendingUndeploy().add(oper);
        }
      }
    }

    // start new containers
    for (PTContainer c: startContainers) {
      ContainerStartRequest dr = new ContainerStartRequest(c);
      containerStartRequests.add(dr);
      pendingAllocation.add(dr.container);
      lastResourceRequest = System.currentTimeMillis();
      for (PTOperator operator: c.getOperators()) {
        operator.setState(PTOperator.State.INACTIVE);
      }
    }

    // (re)deploy affected operators
    // can happen in parallel after buffer server for recovered publishers is reset
    Map<PTContainer, List<PTOperator>> deployGroups = groupByContainer(deploy);
    for (Map.Entry<PTContainer, List<PTOperator>> e: deployGroups.entrySet()) {
      if (!startContainers.contains(e.getKey())) {
        // to reset publishers, clean buffer server past checkpoint so subscribers don't read stale data (including end of stream)
        for (PTOperator operator: e.getValue()) {
          for (PTOperator.PTOutput out: operator.getOutputs()) {
            if (!out.isDownStreamInline()) {
              // following needs to match the concat logic in StramChild
              String sourceIdentifier = Integer.toString(operator.getId()).concat(Component.CONCAT_SEPARATOR).concat(out.portName);
              if (operator.getContainer().getState() == PTContainer.State.ACTIVE) {
                // TODO: unit test - find way to mock this when testing rest of logic
                if (operator.getContainer().bufferServerAddress.getPort() != 0) {
                  BufferServerController bsc = getBufferServerClient(operator);
                  // reset publisher (stale operator may still write data until disconnected)
                  // ensures new subscriber starting to read from checkpoint will wait until publisher redeploy cycle is complete
                  try {
                    bsc.reset(null, sourceIdentifier, 0);
                  }
                  catch (Exception ex) {
                    LOG.error("Failed to reset buffer server {} {}", sourceIdentifier, ex);
                  }
                }
              }
            }
          }
        }
      }

      // add to operators that we expect to deploy
      // TODO: handle concurrent deployment
      LOG.debug("scheduling deploy {} {}", e.getKey().getExternalId(), e.getValue());
      e.getKey().getPendingDeploy().addAll(e.getValue());
    }

    // stop containers that are no longer used
    for (PTContainer c: releaseContainers) {
      if (c.getExternalId() == null)
        continue;
      StramChildAgent sca = containers.get(c.getExternalId());
      if (sca != null) {
        LOG.debug("Container marked for shutdown: {}", c);
        // TODO: set deactivated state and monitor soft shutdown
        //sca.container.setState(PTContainer.State.KILLED);
        sca.shutdownRequested = true;
      }
    }
  }

  @Override
  public void recordEventAsync(StramEvent ev)
  {
    if (eventBus != null) {
      eventBus.publishAsync(ev);
    }
  }

  @Override
  public void dispatch(Runnable r)
  {
    this.eventQueue.add(r);
  }

  public OperatorInfo getOperatorInfo(String operatorId)
  {
    for (PTContainer container: this.plan.getContainers()) {
      for (PTOperator operator : container.getOperators()) {
        if (operatorId.equals(Integer.toString(operator.getId()))) {
          return fillOperatorInfo(operator);
        }
      }
    }
    return null;
  }

  public List<OperatorInfo> getOperatorInfoList()
  {
    List<OperatorInfo> infoList = new ArrayList<OperatorInfo>();

    for (PTContainer container: this.plan.getContainers()) {
      for (PTOperator operator: container.getOperators()) {
        infoList.add(fillOperatorInfo(operator));
      }
    }
    return infoList;
  }

  public static long toWsWindowId(long windowId) {
    // until console handles -1
    return windowId < 0 ? 0 : windowId;
  }

  private OperatorInfo fillOperatorInfo(PTOperator operator)
  {
    OperatorInfo ni = new OperatorInfo();
    ni.container = operator.getContainer().getExternalId();
    ni.host = operator.getContainer().host;
    ni.id = Integer.toString(operator.getId());
    ni.name = operator.getName();
    ni.className = operator.getOperatorMeta().getOperator().getClass().getName();
    ni.status = operator.getState().toString();
    ni.unifierClass = (operator.getUnifier() == null) ? null : operator.getUnifier().getClass().getName();
    ni.logicalName = operator.getOperatorMeta().getName();

    if (operator.stats != null) {
      OperatorStatus os = operator.stats;
      ni.recordingStartTime = os.recordingStartTime;
      ni.totalTuplesProcessed = os.totalTuplesProcessed;
      ni.totalTuplesEmitted = os.totalTuplesEmitted;
      ni.tuplesProcessedPSMA = os.tuplesProcessedPSMA;
      ni.tuplesEmittedPSMA = os.tuplesEmittedPSMA;
      ni.cpuPercentageMA = os.cpuPercentageMA.getAvg();
      ni.latencyMA = os.latencyMA.getAvg();
      ni.failureCount = operator.failureCount;
      ni.recoveryWindowId = toWsWindowId(operator.getRecoveryCheckpoint());
      ni.currentWindowId = toWsWindowId(os.currentWindowId);
      if (os.lastHeartbeat != null) {
        ni.lastHeartbeat = os.lastHeartbeat.getGeneratedTms();
      }
      for (PortStatus ps: os.inputPortStatusList.values()) {
        PortInfo pinfo = new PortInfo();
        pinfo.name = ps.portName;
        pinfo.type = "input";
        pinfo.totalTuples = ps.totalTuples;
        pinfo.tuplesPSMA = (long)ps.tuplesPSMA.getAvg();
        pinfo.bufferServerBytesPSMA = (long)ps.bufferServerBytesPSMA.getAvg();
        ni.addPort(pinfo);
        pinfo.recordingStartTime = ps.recordingStartTime;
      }
      for (PortStatus ps: os.outputPortStatusList.values()) {
        PortInfo pinfo = new PortInfo();
        pinfo.name = ps.portName;
        pinfo.type = "output";
        pinfo.totalTuples = ps.totalTuples;
        pinfo.tuplesPSMA = (long)ps.tuplesPSMA.getAvg();
        pinfo.bufferServerBytesPSMA = (long)ps.bufferServerBytesPSMA.getAvg();
        ni.addPort(pinfo);
      }
    }
    return ni;
  }

  public List<StreamInfo> getStreamInfoList()
  {
    List<StreamInfo> infoList = new ArrayList<StreamInfo>();

    for (PTContainer container: this.plan.getContainers()) {
      for (PTOperator operator: container.getOperators()) {
        List<PTOutput> outputs = operator.getOutputs();
        for (PTOutput output : outputs) {
          StreamInfo si = new StreamInfo();
          si.logicalName = output.logicalStream.getName();
          si.source.operatorId = String.valueOf(operator.getId());
          si.source.portName = output.portName;
          si.locality = output.logicalStream.getLocality();
          for (PTInput input : output.sinks) {
            StreamInfo.Port p = new StreamInfo.Port();
            p.operatorId = String.valueOf(input.target.getId());
            if (input.target.getUnifier() != null) {
              p.portName = StramChild.getUnifierInputPortName(input.portName, operator.getId(), output.portName);
            }
            else {
              p.portName = input.portName;
            }
            si.sinks.add(p);
          }
          infoList.add(si);
        }
      }
    }
    return infoList;
  }

  private static class RecordingRequestFilter implements Predicate<StramToNodeRequest>
  {
    final static Set<StramToNodeRequest.RequestType> MATCH_TYPES = Sets.newHashSet(StramToNodeRequest.RequestType.START_RECORDING, StramToNodeRequest.RequestType.STOP_RECORDING, StramToNodeRequest.RequestType.SYNC_RECORDING);

    @Override
    public boolean apply(@Nullable StramToNodeRequest input)
    {
      return MATCH_TYPES.contains(input.getRequestType());
    }

  }

  private class SetOperatorPropertyRequestFilter implements Predicate<StramToNodeRequest>
  {
    final String propertyKey;

    SetOperatorPropertyRequestFilter(String key)
    {
      this.propertyKey = key;
    }

    @Override
    public boolean apply(@Nullable StramToNodeRequest input)
    {
      return input.getRequestType() == StramToNodeRequest.RequestType.SET_PROPERTY && input.setPropertyKey.equals(propertyKey);
    }

  }

  private void updateOnDeployRequests(PTOperator p, Predicate<StramToNodeRequest> superseded, StramToNodeRequest newRequest)
  {
    // filter existing requests
    List<StramToNodeRequest> cloneRequests = new ArrayList<StramToNodeRequest>(p.deployRequests.size());
    for (StramToNodeRequest existingRequest: p.deployRequests) {
      if (!superseded.apply(existingRequest)) {
        cloneRequests.add(existingRequest);
      }
    }
    // add new request, if any
    if (newRequest != null) {
      cloneRequests.add(newRequest);
    }
    p.deployRequests = Collections.unmodifiableList(cloneRequests);
  }

  private StramChildAgent getContainerAgentFromOperatorId(int operatorId)
  {
    PTOperator oper = plan.getAllOperators().get(operatorId);
    if (oper != null) {
      StramChildAgent sca = containers.get(oper.getContainer().getExternalId());
      if (sca != null) {
        return sca;
      }
    }
    // throw exception that propagates to web client
    throw new NotFoundException("Operator ID " + operatorId + " not found");
  }

  public void startRecording(int operId, String portName)
  {
    StramChildAgent sca = getContainerAgentFromOperatorId(operId);
    StramToNodeRequest request = new StramToNodeRequest();
    request.setOperatorId(operId);
    if (!StringUtils.isBlank(portName)) {
      request.setPortName(portName);
    }
    request.setRequestType(StramToNodeRequest.RequestType.START_RECORDING);
    sca.addOperatorRequest(request);
    PTOperator operator = plan.getAllOperators().get(operId);
    if (operator != null) {
      // restart on deploy
      updateOnDeployRequests(operator, new RecordingRequestFilter(), request);
    }
  }

  public void stopRecording(int operId, String portName)
  {
    StramChildAgent sca = getContainerAgentFromOperatorId(operId);
    StramToNodeRequest request = new StramToNodeRequest();
    request.setOperatorId(operId);
    if (!StringUtils.isBlank(portName)) {
      request.setPortName(portName);
    }
    request.setRequestType(StramToNodeRequest.RequestType.STOP_RECORDING);
    sca.addOperatorRequest(request);
    PTOperator operator = plan.getAllOperators().get(operId);
    if (operator != null) {
      // no stop on deploy, but remove existing start
      updateOnDeployRequests(operator, new RecordingRequestFilter(), null);
    }
  }
/*
  public void syncRecording(int operId, String portName)
  {
    StramChildAgent sca = getContainerAgentFromOperatorId(operId);
    StramToNodeRequest request = new StramToNodeRequest();
    request.setOperatorId(operId);
    if (!StringUtils.isBlank(portName)) {
      request.setPortName(portName);
    }
    request.setRequestType(RequestType.SYNC_RECORDING);
    sca.addOperatorRequest(request);
  }
*/
  public void syncStats()
  {
    statsRecorder.requestSync();
  }

  public void syncEvents()
  {
    eventRecorder.requestSync();
  }

  public void stopContainer(String containerId)
  {
    this.containerStopRequests.put(containerId, containerId);
  }

  public void setOperatorProperty(String operatorName, String propertyName, String propertyValue)
  {
    OperatorMeta logicalOperator = plan.getDAG().getOperatorMeta(operatorName);
    if (logicalOperator == null) {
      throw new IllegalArgumentException("Unknown operator " + operatorName);
    }

    Map<String, String> properties = Collections.singletonMap(propertyName, propertyValue);
    LogicalPlanConfiguration.setOperatorProperties(logicalOperator.getOperator(), properties);
    // need to record this to a log in the future when review history is supported

    List<PTOperator> operators = plan.getOperators(logicalOperator);
    for (PTOperator o: operators) {
      StramChildAgent sca = getContainerAgent(o.getContainer().getExternalId());
      StramToNodeRequest request = new StramToNodeRequest();
      request.setOperatorId(o.getId());
      request.setPropertyKey = propertyName;
      request.setPropertyValue = propertyValue;
      request.setRequestType(StramToNodeRequest.RequestType.SET_PROPERTY);
      sca.addOperatorRequest(request);
      // re-apply to checkpointed state on deploy
      updateOnDeployRequests(o, new SetOperatorPropertyRequestFilter(propertyName), request);
    }
    // should probably not record it here because it's better to get confirmation from the operators first.
    // but right now, the operators do not give confirmation for the requests.  so record it here for now.
    recordEventAsync(new StramEvent.SetOperatorPropertyEvent(operatorName, propertyName, propertyValue));
  }

  /**
   * Set property on a physical operator. The property change is applied asynchronously on the deployed operator.
   * @param operatorId
   * @param propertyName
   * @param propertyValue
   */
  public void setPhysicalOperatorProperty(String operatorId, String propertyName, String propertyValue)
  {
    String operatorName = null;
    int id = Integer.valueOf(operatorId);
    PTOperator o = this.plan.getAllOperators().get(id);
    if (o == null)
      return;

    operatorName = o.getName();
    StramChildAgent sca = getContainerAgent(o.getContainer().getExternalId());
    StramToNodeRequest request = new StramToNodeRequest();
    request.setOperatorId(id);
    request.setPropertyKey = propertyName;
    request.setPropertyValue = propertyValue;
    request.setRequestType(StramToNodeRequest.RequestType.SET_PROPERTY);
    sca.addOperatorRequest(request);
    updateOnDeployRequests(o, new SetOperatorPropertyRequestFilter(propertyName), request);

    // should probably not record it here because it's better to get confirmation from the operators first.
    // but right now, the operators do not give confirmation for the requests. so record it here for now.
    recordEventAsync(new StramEvent.SetPhysicalOperatorPropertyEvent(operatorName, id, propertyName, propertyValue));

  }

  public Map<String, Object> getPhysicalOperatorProperty(String operatorId){
    int id = Integer.valueOf(operatorId);
    PTOperator o = this.plan.getAllOperators().get(id);
    Map<String, Object> m = LogicalPlanConfiguration.getOperatorProperties(o.getOperatorMeta().getOperator());
    m = Maps.newHashMap(m); // clone as map returned is linked to object
    for (StramToNodeRequest existingRequest : o.deployRequests) {
      if (id == existingRequest.operatorId){
        m.put(existingRequest.setPropertyKey, existingRequest.setPropertyValue);
      }
    }
    return m;
  }

  public AttributeMap getApplicationAttributes()
  {
    LogicalPlan lp = getLogicalPlan();
    return lp.getAttributes().clone();
  }

  public AttributeMap getOperatorAttributes(String operatorId)
  {
    OperatorMeta logicalOperator = plan.getDAG().getOperatorMeta(operatorId);
    if (logicalOperator == null) {
      throw new IllegalArgumentException("Invalid operatorId " + operatorId);
    }
    return logicalOperator.getAttributes().clone();
  }

  public AttributeMap getPortAttributes(String operatorId, String portName)
  {
    OperatorMeta logicalOperator = plan.getDAG().getOperatorMeta(operatorId);
    if (logicalOperator == null) {
      throw new IllegalArgumentException("Invalid operatorId " + operatorId);
    }

    Operators.PortMappingDescriptor portMap = new Operators.PortMappingDescriptor();
    Operators.describe(logicalOperator.getOperator(), portMap);
    InputPort<?> inputPort = portMap.inputPorts.get(portName).component;
    if (inputPort != null) {
      return logicalOperator.getMeta(inputPort).getAttributes().clone();
    } else {
      OutputPort<?> outputPort = portMap.outputPorts.get(portName).component;
      if (outputPort == null) {
        throw new IllegalArgumentException("Invalid port name " + portName);
      }
      return logicalOperator.getMeta(outputPort).getAttributes().clone();
    }
  }

  public LogicalPlan getLogicalPlan()
  {
    return plan.getDAG();
  }

  /**
   * Asynchronously process the logical, physical plan and execution layer changes.
   * Caller can use the returned future to block until processing is complete.
   * @param requests
   * @return
   * @throws Exception
   */
  public FutureTask<Object> logicalPlanModification(List<LogicalPlanRequest> requests) throws Exception
  {
    // delegate processing to dispatch thread
    FutureTask<Object> future = new FutureTask<Object>(new LogicalPlanChangeRunnable(requests));
    dispatch(future);
    //LOG.info("Scheduled plan changes: {}", requests);
    return future;
  }

  private class LogicalPlanChangeRunnable implements java.util.concurrent.Callable<Object> {
    final List<LogicalPlanRequest> requests;

    private LogicalPlanChangeRunnable(List<LogicalPlanRequest> requests)
    {
      this.requests = requests;
    }

    @Override
    public Object call() throws Exception {
      // clone logical plan, for dry run and validation
      LOG.info("Begin plan changes: {}", requests);
      LogicalPlan lp = plan.getDAG();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      LogicalPlan.write(lp, bos);
      bos.flush();
      ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
      lp = LogicalPlan.read(bis);

      PlanModifier pm = new PlanModifier(lp);
      for (LogicalPlanRequest request : requests) {
        LOG.debug("Dry run plan change: {}", request);
        request.execute(pm);
      }

      lp.validate();

      // perform changes on live plan
      pm = new PlanModifier(plan);
      for (LogicalPlanRequest request : requests) {
        request.execute(pm);

        // record an event for the request.  however, we should probably record these when we get a confirmation.
        recordEventAsync(new StramEvent.ChangeLogicalPlanEvent(request));
      }
      pm.applyChanges(StreamingContainerManager.this);
      LOG.info("Plan changes applied: {}", requests);
      return null;
    }
  }

  public CriticalPathInfo getCriticalPathInfo()
  {
    return criticalPathInfo;
  }

  public AlertsManager getAlertsManager()
  {
    return alertsManager;
  }

}

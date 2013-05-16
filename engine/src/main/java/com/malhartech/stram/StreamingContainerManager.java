/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.malhartech.api.BackupAgent;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.api.DAG.OperatorMeta;
import com.malhartech.api.DAGContext;
import com.malhartech.engine.OperatorStats;
import com.malhartech.engine.OperatorStats.PortStats;
import com.malhartech.stram.PhysicalPlan.PTContainer;
import com.malhartech.stram.PhysicalPlan.PTInput;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.PhysicalPlan.PTOperator.State;
import com.malhartech.stram.PhysicalPlan.PTOutput;
import com.malhartech.stram.PhysicalPlan.PlanContext;
import com.malhartech.stram.PhysicalPlan.StatsHandler;
import com.malhartech.stram.StramChildAgent.ContainerStartRequest;
import com.malhartech.stram.StramChildAgent.OperatorStatus;
import com.malhartech.stram.StramChildAgent.PortStatus;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;
import com.malhartech.stram.webapp.OperatorInfo;
import com.malhartech.stram.webapp.PortInfo;
import com.malhartech.util.AttributeMap;
import com.malhartech.common.Pair;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 */
public class StreamingContainerManager implements PlanContext
{
  private final static Logger LOG = LoggerFactory.getLogger(StreamingContainerManager.class);
  private long windowStartMillis = System.currentTimeMillis();
  private int heartbeatTimeoutMillis = 30000;
  private int maxWindowsBehindForStats = 100;
  private int recordStatsInterval = 0;
  private long lastRecordStatsTime = 0;
  private HdfsStatsRecorder statsRecorder;
  private final int operatorMaxAttemptCount = 5;
  private final AttributeMap<DAGContext> appAttributes;
  //private final int checkpointIntervalMillis;
  private final String appPath;
  private final String checkpointFsPath;
  private final String statsFsPath;
  protected final Map<String, String> containerStopRequests = new ConcurrentHashMap<String, String>();
  protected final ConcurrentLinkedQueue<ContainerStartRequest> containerStartRequests = new ConcurrentLinkedQueue<ContainerStartRequest>();
  protected final ConcurrentLinkedQueue<Runnable> eventQueue = new ConcurrentLinkedQueue<Runnable>();
  protected String shutdownDiagnosticsMessage = "";
  protected boolean forcedShutdown = false;
  private long lastResourceRequest = 0;
  private final Map<String, StramChildAgent> containers = new ConcurrentHashMap<String, StramChildAgent>();
  private final PhysicalPlan plan;
  private final List<Pair<PTOperator, Long>> purgeCheckpoints = new ArrayList<Pair<PTOperator, Long>>();
  private final ConcurrentSkipListMap<Long, Map<Integer, EndWindowStats>> endWindowStatsOperatorMap = new ConcurrentSkipListMap<Long, Map<Integer, EndWindowStats>>();
  private long committedWindowId;

  private static class EndWindowStats
  {
    long emitTimestamp = -1;
    HashMap<String, Long> dequeueTimestamps = new HashMap<String, Long>();
  }

  public StreamingContainerManager(DAG dag)
  {
    this.plan = new PhysicalPlan(dag, this);
    this.appAttributes = dag.getAttributes();

    appAttributes.attr(DAG.STRAM_WINDOW_SIZE_MILLIS).setIfAbsent(500);
    // try to align to it pleases eyes.
    windowStartMillis -= (windowStartMillis % 1000);

    appAttributes.attr(DAG.STRAM_APP_PATH).setIfAbsent("stram/" + System.currentTimeMillis());
    this.appPath = appAttributes.attr(DAG.STRAM_APP_PATH).get();
    this.checkpointFsPath = this.appPath + "/" + DAG.SUBDIR_CHECKPOINTS;
    this.statsFsPath = this.appPath + "/" + DAG.SUBDIR_STATS;

    appAttributes.attr(DAG.STRAM_CHECKPOINT_WINDOW_COUNT).setIfAbsent(30000 / appAttributes.attr(DAG.STRAM_WINDOW_SIZE_MILLIS).get());
    //this.checkpointIntervalMillis = appAttributes.attr(DAG.STRAM_CHECKPOINT_WINDOW_COUNT).get() * appAttributes.attr(DAG.STRAM_WINDOW_SIZE_MILLIS).get();
    this.heartbeatTimeoutMillis = appAttributes.attrValue(DAG.STRAM_HEARTBEAT_TIMEOUT_MILLIS, this.heartbeatTimeoutMillis);

    appAttributes.attr(DAG.STRAM_MAX_WINDOWS_BEHIND_FOR_STATS).setIfAbsent(100);
    this.maxWindowsBehindForStats = appAttributes.attr(DAG.STRAM_MAX_WINDOWS_BEHIND_FOR_STATS).get();

    appAttributes.attr(DAG.STRAM_RECORD_STATS_INTERVAL_MILLIS).setIfAbsent(0);
    this.recordStatsInterval = appAttributes.attr(DAG.STRAM_RECORD_STATS_INTERVAL_MILLIS).get();
    if (this.recordStatsInterval > 0) {
      statsRecorder = new HdfsStatsRecorder();
      statsRecorder.setBasePath(this.statsFsPath);
      statsRecorder.setup();
    }
  }

  protected PhysicalPlan getPhysicalPlan()
  {
    return plan;
  }

  /**
   * Check periodically that child containers phone home.
   * This is run by the App Master thread (only accessed by one thread).
   */
  public void monitorHeartbeat()
  {
    long currentTms = System.currentTimeMillis();

    // look for resource allocation timeout
    for (PTContainer c: plan.getContainers()) {
      // TODO: single state for resource requested
      if (c.getState() == PTContainer.State.NEW || c.getState() == PTContainer.State.KILLED) {
        // look for resource allocation timeout
        if (lastResourceRequest + appAttributes.attrValue(DAG.STRAM_ALLOCATE_RESOURCE_TIMEOUT_MILLIS, DAG.DEFAULT_STRAM_ALLOCATE_RESOURCE_TIMEOUT_MILLIS) < currentTms) {
          String msg = String.format("Shutdown due to resource allocation timeout (%s ms) with container %s (state is %s)", currentTms - lastResourceRequest, c.containerId, c.getState().name());
          LOG.warn(msg);
          forcedShutdown = true;
          shutdownAllContainers(msg);
        }
        else {
          LOG.debug("Waiting for resource: {}m {}", c.getRequiredMemoryMB(), c);
        }
      }
      else if (c.containerId != null) {
        StramChildAgent cs = containers.get(c.containerId);
        if (!cs.isComplete && cs.lastHeartbeatMillis + heartbeatTimeoutMillis < currentTms) {
          // TODO: handle containers hung in deploy requests
          if (cs.lastHeartbeatMillis > 0 && !cs.hasPendingWork() && !isApplicationIdle()) {
            // request stop (kill) as process may still be hanging around (would have been detected by Yarn otherwise)
            LOG.info("Container {}@{} heartbeat timeout ({} ms).", new Object[] {c.containerId, c.host, currentTms - cs.lastHeartbeatMillis});
            containerStopRequests.put(c.containerId, c.containerId);
          }
        }
      }
    }

    processEvents();
    committedWindowId = updateCheckpoints();
    calculateEndWindowStats();
    if (recordStatsInterval > 0 && (lastRecordStatsTime + recordStatsInterval <= System.currentTimeMillis())) {
      recordStats();
    }
  }

  private void recordStats()
  {
    statsRecorder.recordContainers(containers);
    statsRecorder.recordOperators(getOperatorInfoList());
    lastRecordStatsTime = System.currentTimeMillis();
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

      Set<Integer> allCurrentOperators = new TreeSet<Integer>();
      for (PTOperator o: plan.getAllOperators()) {
        allCurrentOperators.add(o.getId());
      }
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
            // they are equal.  start latency calculation
            List<OperatorMeta> rootOperatorMetas = plan.getRootOperators();
            Set<PTOperator> endWindowStatsVisited = new HashSet<PTOperator>();
            for (OperatorMeta root: rootOperatorMetas) {
              List<PTOperator> rootOperators = plan.getOperators(root);
              for (PTOperator rootOperator: rootOperators) {
                // DFS for visiting the nodes for latency calculation
                calculateLatency(rootOperator, endWindowStatsMap, endWindowStatsVisited);
              }
            }
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

  private void calculateLatency(PTOperator oper, Map<Integer, EndWindowStats> endWindowStatsMap, Set<PTOperator> endWindowStatsVisited)
  {
    endWindowStatsVisited.add(oper);
    OperatorStatus operatorStatus = getOperatorStatus(oper);
    if (operatorStatus == null) {
      LOG.info("Operator status for operator " + oper.getId() + " does not exist yet.");
      return;
    }

    EndWindowStats endWindowStats = endWindowStatsMap.get(oper.getId());
    if (endWindowStats == null) {
      LOG.info("End window stats is null for operator {}, probably a new operator after partitioning");
      return;
    }

    // find the maximum end window emit time from all input ports
    long upstreamMaxEmitTimestamp = -1;
    for (PTInput input: oper.inputs) {
      if (input.source.source instanceof PTOperator) {
        PTOperator upstreamOp = (PTOperator)input.source.source;
        EndWindowStats upstreamEndWindowStats = endWindowStatsMap.get(upstreamOp.getId());
        if (upstreamEndWindowStats == null) {
          LOG.info("End window stats is null for operator {}");
          return;
        }
        if (upstreamEndWindowStats.emitTimestamp > upstreamMaxEmitTimestamp) {
          upstreamMaxEmitTimestamp = upstreamEndWindowStats.emitTimestamp;
        }
      }
    }

    if (upstreamMaxEmitTimestamp > 0) {
      operatorStatus.latencyMA.add(endWindowStats.emitTimestamp - upstreamMaxEmitTimestamp);
    }

    for (PTOutput output: oper.outputs) {
      for (PTInput input: output.sinks) {
        if (input.target instanceof PTOperator) {
          PTOperator downStreamOp = (PTOperator)input.target;
          if (!endWindowStatsVisited.contains(downStreamOp)) {
            calculateLatency(downStreamOp, endWindowStatsMap, endWindowStatsVisited);
          }
        }
      }
    }
  }

  private OperatorStatus getOperatorStatus(PTOperator operator)
  {
    StramChildAgent sca = containers.get(operator.container.containerId);
    if (sca == null) {
      return null;
    }
    return sca.operators.get(operator.getId());
  }

  public int processEvents()
  {
    int count = 0;
    Runnable command;
    while ((command = this.eventQueue.poll()) != null) {
      try {
        command.run();
        count++;
      }
      catch (Exception e) {
        // TODO: handle error
        LOG.error("Failed to execute {} {}", command, e);
      }
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

    LOG.info("Initiating recovery for container {}@{}", containerId, cs.container.host);

    cs.container.setState(PTContainer.State.KILLED);
    cs.container.bufferServerAddress = null;

    // building the checkpoint dependency,
    // downstream operators will appear first in map
    LinkedHashSet<PTOperator> checkpoints = new LinkedHashSet<PTOperator>();

    MutableLong ml = new MutableLong();
    for (PTOperator node: cs.container.operators) {
      // TODO: traverse inline upstream operators
      updateRecoveryCheckpoints(node, checkpoints, ml);
    }

    // redeploy cycle for all affected operators
    deploy(Collections.<PTContainer>emptySet(), checkpoints, Sets.newHashSet(cs.container), checkpoints);
  }

  public void markComplete(String containerId)
  {
    StramChildAgent cs = containers.get(containerId);
    if (cs == null) {
      LOG.warn("Completion status for unknown container {}", containerId);
      return;
    }
    cs.isComplete = true;
  }

  public static class ContainerResource
  {
    public final String containerId;
    public final String host;
    public final int memoryMB;
    public final int priority;

    public ContainerResource(int priority, String containerId, String host, int memoryMB)
    {
      this.containerId = containerId;
      this.host = host;
      this.memoryMB = memoryMB;
      this.priority = priority;
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

  private PTContainer matchContainer(ContainerResource resource)
  {
    PTContainer match = null;
    // match container waiting for resource
    for (PTContainer c: plan.getContainers()) {
      if (c.getState() == PTContainer.State.NEW || c.getState() == PTContainer.State.KILLED) {
        if (c.getResourceRequestPriority() == resource.priority) {
          return c;
        }
        /*
         if (container.getRequiredMemoryMB() <= resource.memoryMB) {
         if (match == null || match.getRequiredMemoryMB() < container.getRequiredMemoryMB()) {
         match = container;
         }
         }
         */
      }
    }
    return match;
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
    PTContainer container = matchContainer(resource);
    if (container == null) {
      LOG.debug("No container matching allocated resource {}", resource);
      return null;
    }

    container.setState(PTContainer.State.ALLOCATED);
    if (container.containerId != null) {
      LOG.info("Removing existing container agent {}", container.containerId);
      this.containers.remove(container.containerId);
    }
    container.containerId = resource.containerId;
    container.host = resource.host;
    container.bufferServerAddress = bufferServerAddr;
    container.setAllocatedMemoryMB(resource.memoryMB);

    StramChildAgent sca = new StramChildAgent(container, newStreamingContainerContext());
    containers.put(resource.containerId, sca);
    return sca;
  }

  private StreamingContainerContext newStreamingContainerContext()
  {
    StreamingContainerContext scc = new StreamingContainerContext();
    scc.applicationAttributes = this.appAttributes;
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

  /**
   * process the heartbeat from each container.
   * called by the RPC thread for each container. (i.e. called by multiple threads)
   *
   * @param heartbeat
   * @return
   */
  public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat heartbeat)
  {
    boolean containerIdle = true;
    long currentTimeMillis = System.currentTimeMillis();

    StramChildAgent sca = this.containers.get(heartbeat.getContainerId());
    if (sca == null) {
      // could be orphaned container that was replaced and needs to terminate
      LOG.error("Unknown container " + heartbeat.getContainerId());
      ContainerHeartbeatResponse response = new ContainerHeartbeatResponse();
      response.shutdown = true;
      return response;
    }

    //LOG.debug("{} {} {}", new Object[]{sca.container.containerId, sca.container.bufferServerAddress, sca.container.getState()});
    if (sca.container.getState() != PTContainer.State.ACTIVE) {
      // capture dynamically assigned address from container
      if (sca.container.bufferServerAddress == null && heartbeat.bufferServerHost != null) {
        sca.container.bufferServerAddress = InetSocketAddress.createUnresolved(heartbeat.bufferServerHost, heartbeat.bufferServerPort);
        LOG.info("Container {} buffer server: {}", sca.container.containerId, sca.container.bufferServerAddress);
      }
      sca.container.setState(PTContainer.State.ACTIVE);
      sca.jvmName = heartbeat.jvmName;
    }

    if (heartbeat.restartRequested) {
      LOG.error("Container {} restart request", sca.container.containerId);
      containerStopRequests.put(sca.container.containerId, sca.container.containerId);
    }

    sca.memoryMBFree = heartbeat.memoryMBFree;

    long elapsedMillis = currentTimeMillis - sca.lastHeartbeatMillis;

    for (StreamingNodeHeartbeat shb: heartbeat.getDnodeEntries()) {

      OperatorStatus status = sca.updateOperatorStatus(shb);
      if (status == null) {
        LOG.error("Heartbeat for unknown operator {} (container {})", shb.getNodeId(), heartbeat.getContainerId());
        continue;
      }

      //LOG.debug("heartbeat {}/{}@{}: {} {}", new Object[] { shb.getNodeId(), status.operator.getName(), heartbeat.getContainerId(), shb.getState(),
      //    Codec.getStringWindowId(shb.getLastBackupWindowId()) });

      StreamingNodeHeartbeat previousHeartbeat = status.lastHeartbeat;
      status.lastHeartbeat = shb;

      if (shb.getState().compareTo(DNodeState.FAILED.name()) == 0) {
        // count failure transitions *->FAILED, applies to initialization as well as intermittent failures
        if (previousHeartbeat == null || DNodeState.FAILED.name().compareTo(previousHeartbeat.getState()) != 0) {
          status.operator.failureCount++;
          LOG.warn("Operator failure: {} count: {}", status.operator, status.operator.failureCount);
          Integer maxAttempts = status.operator.getOperatorMeta().getAttributes().attrValue(OperatorContext.RECOVERY_ATTEMPTS, this.operatorMaxAttemptCount);
          if (status.operator.failureCount <= maxAttempts) {
            // restart entire container in attempt to recover operator
            // in the future a more sophisticated recovery strategy could
            // involve initial redeploy attempt(s) of affected operator in
            // existing container or sandbox container for just the operator
            LOG.error("Issuing container stop to restart after operator failure {}", status.operator);
            containerStopRequests.put(sca.container.containerId, sca.container.containerId);
          }
          else {
            String msg = String.format("Shutdown after reaching failure threshold for %s", status.operator);
            LOG.warn(msg);
            forcedShutdown = true;
            shutdownAllContainers(msg);
          }
        }
      }

      if (!status.isIdle()) {
        containerIdle = false;

        long tuplesProcessed = 0;
        long tuplesEmitted = 0;
        long totalCpuTimeUsed = 0;
        long maxDequeueTimestamp = -1;
        List<OperatorStats> statsList = shb.getWindowStats();
        HashMap<String, MutableLong> portToTuples = new HashMap<String, MutableLong>();

        for (OperatorStats stats: statsList) {
          /* report checkpointedWindowId status of the operator */
          if (status.operator.recoveryCheckpoint < stats.checkpointedWindowId) {
            addCheckpoint(status.operator, stats.checkpointedWindowId);
          }

          /* report all the other stuff */

          endWindowStatsOperatorMap.putIfAbsent(stats.windowId, new ConcurrentHashMap<Integer, EndWindowStats>());
          EndWindowStats endWindowStats = new EndWindowStats();
          Collection<PortStats> ports = stats.inputPorts;
          if (ports != null) {
            for (PortStats s: ports) {
              PortStatus ps = status.inputPortStatusList.get(s.portname);
              if (ps == null) {
                ps = sca.new PortStatus();
                ps.portName = s.portname;
                status.inputPortStatusList.put(s.portname, ps);
              }
              ps.totalTuples += s.processedCount;
              if (portToTuples.containsKey(s.portname)) {
                portToTuples.get(s.portname).add(s.processedCount);
              }
              else {
                portToTuples.put(s.portname, new MutableLong(s.processedCount));
              }

              tuplesProcessed += s.processedCount;
              endWindowStats.dequeueTimestamps.put(s.portname, s.endWindowTimestamp);
              if (s.endWindowTimestamp > maxDequeueTimestamp) {
                maxDequeueTimestamp = s.endWindowTimestamp;
              }
            }
          }

          ports = stats.outputPorts;
          if (ports != null) {

            for (PortStats s: ports) {
              PortStatus ps = status.outputPortStatusList.get(s.portname);
              if (ps == null) {
                ps = sca.new PortStatus();
                ps.portName = s.portname;
                status.outputPortStatusList.put(s.portname, ps);
              }
              ps.totalTuples += s.processedCount;
              if (portToTuples.containsKey(s.portname)) {
                portToTuples.get(s.portname).add(s.processedCount);
              }
              else {
                portToTuples.put(s.portname, new MutableLong(s.processedCount));
              }

              tuplesEmitted += s.processedCount;
            }
            if (ports.size() > 0) {
              endWindowStats.emitTimestamp = ports.iterator().next().endWindowTimestamp;
            }
          }

          // for output operator, just take the maximum dequeue time for emit timestamp.
          if (endWindowStats.emitTimestamp < 0) {
            endWindowStats.emitTimestamp = maxDequeueTimestamp;
          }

          status.currentWindowId = stats.windowId;
          totalCpuTimeUsed += stats.cpuTimeUsed;
          /*
           if (endWindowStatsOperatorMap.putIfAbsent(stats.windowId, new ConcurrentHashMap<Integer, EndWindowStats>()) == null) {
           LOG.warn("Putting new map for window id {} for node id {}", stats.windowId, shb.getNodeId());
           }
           */
          endWindowStatsOperatorMap.putIfAbsent(stats.windowId, new ConcurrentHashMap<Integer, EndWindowStats>());
          Map<Integer, EndWindowStats> endWindowStatsMap = endWindowStatsOperatorMap.get(stats.windowId);
          endWindowStatsMap.put(shb.getNodeId(), endWindowStats);
        }

        status.totalTuplesProcessed += tuplesProcessed;
        status.totalTuplesEmitted += tuplesEmitted;
        if (elapsedMillis > 0) {
          status.tuplesProcessedPSMA10.add((tuplesProcessed * 1000) / elapsedMillis);
          status.tuplesEmittedPSMA10.add((tuplesEmitted * 1000) / elapsedMillis);
          status.cpuPercentageMA10.add((double)totalCpuTimeUsed * 100 / (elapsedMillis * 1000000));
          for (PortStatus ps: status.inputPortStatusList.values()) {
            if (portToTuples.containsKey(ps.portName)) {
              ps.tuplesPSMA10.add(portToTuples.get(ps.portName).longValue() * 1000 / elapsedMillis);
            }
            Long numBytes = shb.getBufferServerBytes().get(ps.portName);
            if (numBytes != null) {
              ps.bufferServerBytesPSMA10.add(numBytes * 1000 / elapsedMillis);
            }
          }
          for (PortStatus ps: status.outputPortStatusList.values()) {
            if (portToTuples.containsKey(ps.portName)) {
              ps.tuplesPSMA10.add(portToTuples.get(ps.portName).longValue() * 1000 / elapsedMillis);
            }
            Long numBytes = shb.getBufferServerBytes().get(ps.portName);
            if (numBytes != null) {
              ps.bufferServerBytesPSMA10.add(numBytes * 1000 / elapsedMillis);
            }
          }
          if (status.operator.statsMonitors != null) {
            long tps = status.operator.inputs.isEmpty() ? status.tuplesEmittedPSMA10.getAvg() : status.tuplesProcessedPSMA10.getAvg();
            for (StatsHandler sm: status.operator.statsMonitors) {
              sm.onThroughputUpdate(status.operator, tps);
              sm.onCpuPercentageUpdate(status.operator, status.cpuPercentageMA10.getAvg());
            }
          }
        }
      }
      status.recordingNames = shb.getRecordingNames();
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
    for (StramChildAgent csa: this.containers.values()) {
      if (!csa.isIdle()) {
        return false;
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
            while (li.hasNext() && li.next().longValue() < backupWindowId);
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
   * @return Checkpoint that can be used to recover (along with dependencies in visitedCheckpoints).
   */
  public long updateRecoveryCheckpoints(PTOperator operator, Set<PTOperator> visited, MutableLong committedWindowId)
  {
    if (operator.recoveryCheckpoint < committedWindowId.longValue()) {
      committedWindowId.setValue(operator.recoveryCheckpoint);
    }
    // checkpoint frozen until deployment complete
    if (operator.getState() == State.PENDING_DEPLOY) {
      return operator.recoveryCheckpoint;
    }

    long maxCheckpoint = operator.getRecentCheckpoint();

    // find smallest most recent subscriber checkpoint
    for (PTOutput out: operator.outputs) {
      for (PhysicalPlan.PTInput sink: out.sinks) {
        PTOperator sinkOperator = (PTOperator)sink.target;
        if (!visited.contains(sinkOperator)) {
          // downstream traversal
          updateRecoveryCheckpoints(sinkOperator, visited, committedWindowId);
        }
        maxCheckpoint = Math.min(maxCheckpoint, sinkOperator.recoveryCheckpoint);
      }
    }

    // find commit point for downstream dependency, remove previous checkpoints
    long c1 = 0;
    synchronized (operator.checkpointWindows) {
      if (!operator.checkpointWindows.isEmpty()) {
        if ((c1 = operator.checkpointWindows.getFirst().longValue()) <= maxCheckpoint) {
          long c2 = 0;
          while (operator.checkpointWindows.size() > 1 && (c2 = operator.checkpointWindows.get(1).longValue()) <= maxCheckpoint) {
            operator.checkpointWindows.removeFirst();
            //LOG.debug("Checkpoint to purge: operator={} windowId={}", operator.getId(), c1);
            this.purgeCheckpoints.add(new Pair<PTOperator, Long>(operator, c1));
            c1 = c2;
          }
        }
        else {
          c1 = 0;
        }
      }
    }
    visited.add(operator);
    //LOG.debug("Operator {} checkpoints: commit {} recent {}", new Object[] {operator.getId(), c1, operator.checkpointWindows});
    return operator.recoveryCheckpoint = c1;
  }

  /**
   * Visit all operators to update current checkpoint based on updated downstream state.
   * Purge older checkpoints that are no longer needed.
   */
  private long updateCheckpoints()
  {
    MutableLong committedWindowId = new MutableLong(Long.MAX_VALUE);

    Set<PTOperator> visitedCheckpoints = new LinkedHashSet<PTOperator>();
    for (OperatorMeta logicalOperator: plan.getRootOperators()) {
      List<PTOperator> operators = plan.getOperators(logicalOperator);
      if (operators != null) {
        for (PTOperator operator: operators) {
          updateRecoveryCheckpoints(operator, visitedCheckpoints, committedWindowId);
        }
      }
    }
    purgeCheckpoints();

    return committedWindowId.longValue();
  }

  private BufferServerController getBufferServerClient(PTOperator operator)
  {
    BufferServerController bsc = new BufferServerController(operator.getLogicalId());
    InetSocketAddress address = operator.container.bufferServerAddress;
    StramChild.eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsc);
    return bsc;
  }

  private void purgeCheckpoints()
  {
    BackupAgent ba = new HdfsBackupAgent(new Configuration(), checkpointFsPath, StramUtils.getNodeSerDe(null));
    for (Pair<PTOperator, Long> p: purgeCheckpoints) {
      PTOperator operator = p.getFirst();
      try {
        ba.delete(operator.getId(), p.getSecond());
      }
      catch (Exception e) {
        LOG.error("Failed to purge checkpoint " + p, e);
      }
      // purge stream state when using buffer server
      for (PTOutput out: operator.outputs) {
        if (!out.isDownStreamInline()) {
          // following needs to match the concat logic in StramChild
          String sourceIdentifier = Integer.toString(operator.getId()).concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(out.portName);
          // purge everything from buffer server prior to new checkpoint
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
  public BackupAgent getBackupAgent()
  {
    return new HdfsBackupAgent(new Configuration(), this.checkpointFsPath, StramUtils.getNodeSerDe(null));
  }

  private Map<PTContainer, List<PTOperator>> groupByContainer(Collection<PTOperator> operators)
  {
    Map<PTContainer, List<PTOperator>> m = new HashMap<PTContainer, List<PTOperator>>();
    for (PTOperator node: operators) {
      List<PTOperator> nodes = m.get(node.container);
      if (nodes == null) {
        nodes = new ArrayList<PhysicalPlan.PTOperator>();
        m.put(node.container, nodes);
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
      if (!startContainers.contains(e.getKey()) && !releaseContainers.contains(e.getKey())) {
        e.getKey().pendingUndeploy.addAll(e.getValue());
      }
    }

    // start new containers
    for (PTContainer c: startContainers) {
      ContainerStartRequest dr = new ContainerStartRequest(c);
      containerStartRequests.add(dr);
      lastResourceRequest = System.currentTimeMillis();
      for (PTOperator operator: c.operators) {
        operator.setState(PTOperator.State.INACTIVE);
      }
    }

    // (re)deploy affected operators
    // can happen in parallel after buffer server state for recovered publishers is reset
    Map<PTContainer, List<PTOperator>> deployGroups = groupByContainer(deploy);
    for (Map.Entry<PTContainer, List<PTOperator>> e: deployGroups.entrySet()) {
      if (!startContainers.contains(e.getKey())) {
        // to reset publishers, clean buffer server past checkpoint so subscribers don't read stale data (including end of stream)
        for (PTOperator operator: e.getValue()) {
          for (PTOutput out: operator.outputs) {
            if (!out.isDownStreamInline()) {
              // following needs to match the concat logic in StramChild
              String sourceIdentifier = Integer.toString(operator.getId()).concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(out.portName);
              // TODO: find way to mock this when testing rest of logic
              if (operator.container.bufferServerAddress.getPort() != 0) {
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

      // add to operators that we expect to deploy
      LOG.debug("scheduling deploy {} {}", e.getKey(), e.getValue());
      e.getKey().pendingDeploy.addAll(e.getValue());
    }

    // stop containers that are no longer used
    for (PTContainer c: releaseContainers) {
      StramChildAgent sca = containers.get(c.containerId);
      if (sca != null) {
        LOG.debug("Container marked for shutdown: {}", c);
        // TODO: set deactivated state and monitor soft shutdown
        sca.shutdownRequested = true;
      }
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
      String containerId = container.containerId;
      StramChildAgent sca = containerId != null ? this.containers.get(container.containerId) : null;

      for (PTOperator operator: container.operators) {
        if (operatorId.equals(Integer.toString(operator.getId()))) {
          OperatorStatus os = (sca != null) ? sca.operators.get(operator.getId()) : null;
          return fillOperatorInfo(operator, os);
        }
      }
    }
    return null;
  }

  public ArrayList<OperatorInfo> getOperatorInfoList()
  {
    ArrayList<OperatorInfo> infoList = new ArrayList<OperatorInfo>();

    for (PTContainer container: this.plan.getContainers()) {

      String containerId = container.containerId;
      StramChildAgent sca = containerId != null ? this.containers.get(container.containerId) : null;

      for (PTOperator operator: container.operators) {
        OperatorStatus os = (sca != null) ? sca.operators.get(operator.getId()) : null;
        infoList.add(fillOperatorInfo(operator, os));
      }
    }
    return infoList;
  }

  private OperatorInfo fillOperatorInfo(PTOperator operator, OperatorStatus os)
  {
    OperatorInfo ni = new OperatorInfo();
    ni.container = operator.container.containerId;
    ni.host = operator.container.host;
    ni.id = Integer.toString(operator.getId());
    ni.name = operator.getName();
    ni.status = operator.getState().toString();

    if (os != null) {
      ni.totalTuplesProcessed = os.totalTuplesProcessed;
      ni.totalTuplesEmitted = os.totalTuplesEmitted;
      ni.tuplesProcessedPSMA10 = os.tuplesProcessedPSMA10.getAvg();
      ni.tuplesEmittedPSMA10 = os.tuplesEmittedPSMA10.getAvg();
      ni.cpuPercentageMA10 = os.cpuPercentageMA10.getAvg();
      ni.latencyMA = os.latencyMA.getAvg();
      ni.failureCount = os.operator.failureCount;
      ni.recoveryWindowId = os.operator.recoveryCheckpoint & 0xffffffffL;
      ni.currentWindowId = os.currentWindowId & 0xffffffffL;
      ni.recordingNames = os.recordingNames;
      if (os.lastHeartbeat != null) {
        ni.lastHeartbeat = os.lastHeartbeat.getGeneratedTms();
      }
      for (PortStatus ps: os.inputPortStatusList.values()) {
        PortInfo pinfo = new PortInfo();
        pinfo.name = ps.portName;
        pinfo.totalTuples = ps.totalTuples;
        pinfo.tuplesPSMA10 = ps.tuplesPSMA10.getAvg();
        pinfo.bufferServerBytesPSMA10 = ps.bufferServerBytesPSMA10.getAvg();
        ni.addInputPort(pinfo);
      }
      for (PortStatus ps: os.outputPortStatusList.values()) {
        PortInfo pinfo = new PortInfo();
        pinfo.name = ps.portName;
        pinfo.totalTuples = ps.totalTuples;
        pinfo.tuplesPSMA10 = ps.tuplesPSMA10.getAvg();
        pinfo.bufferServerBytesPSMA10 = ps.bufferServerBytesPSMA10.getAvg();
        ni.addOutputPort(pinfo);
      }
    }
    return ni;
  }

  private static class RecordingRequestFilter implements Predicate<StramToNodeRequest>
  {
    final static Set<StramToNodeRequest.RequestType> MATCH_TYPES = Sets.newHashSet(RequestType.START_RECORDING, RequestType.STOP_RECORDING, RequestType.SYNC_RECORDING);

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
      return input.getRequestType() == RequestType.SET_PROPERTY && input.setPropertyKey.equals(propertyKey);
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
    // Thomas, please change it when you get a chance.  -- David
    for (StramChildAgent container: containers.values()) {
      if (container.operators.containsKey(operatorId)) {
        return container;
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
    request.setRequestType(RequestType.START_RECORDING);
    sca.addOperatorRequest(request);
    OperatorStatus os = sca.operators.get(operId);
    if (os != null) {
      // restart on deploy
      updateOnDeployRequests(os.operator, new RecordingRequestFilter(), request);
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
    request.setRequestType(RequestType.STOP_RECORDING);
    sca.addOperatorRequest(request);
    OperatorStatus os = sca.operators.get(operId);
    if (os != null) {
      // no stop on deploy, but remove existing start
      updateOnDeployRequests(os.operator, new RecordingRequestFilter(), null);
    }
  }

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

  public void stopContainer(String containerId)
  {
    this.containerStopRequests.put(containerId, containerId);
  }

  public void setOperatorProperty(String operatorId, String propertyName, String propertyValue)
  {
    OperatorMeta logicalOperator = plan.getDAG().getOperatorMeta(operatorId);
    if (logicalOperator == null) {
      throw new IllegalArgumentException("Invalid operatorId " + operatorId);
    }

    List<PTOperator> operators = plan.getOperators(logicalOperator);
    for (PTOperator o: operators) {
      StramChildAgent sca = getContainerAgent(o.getContainer().containerId);
      StramToNodeRequest request = new StramToNodeRequest();
      request.setOperatorId(o.getId());
      request.setPropertyKey = propertyName;
      request.setPropertyValue = propertyValue;
      request.setRequestType(RequestType.SET_PROPERTY);
      sca.addOperatorRequest(request);
      // restart on deploy
      updateOnDeployRequests(o, new SetOperatorPropertyRequestFilter(propertyName), request);
    }

  }

}

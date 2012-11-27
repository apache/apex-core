/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.api.DAG.OperatorWrapper;
import com.malhartech.api.DAG.StreamDecl;
import com.malhartech.engine.OperatorStats;
import com.malhartech.engine.OperatorStats.PortStats;
import com.malhartech.stram.PhysicalPlan.PTContainer;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.PhysicalPlan.PTOutput;
import com.malhartech.stram.StramChildAgent.ContainerStartRequest;
import com.malhartech.stram.StramChildAgent.DeployRequest;
import com.malhartech.stram.StramChildAgent.OperatorStatus;
import com.malhartech.stram.StramChildAgent.UndeployRequest;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;
import com.malhartech.stram.webapp.OperatorInfo;
import com.malhartech.util.Pair;
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

public class StreamingContainerManager
{
  private final static Logger LOG = LoggerFactory.getLogger(StreamingContainerManager.class);
  private long windowStartMillis = System.currentTimeMillis();
  private int windowSizeMillis = 500;
  private final int heartbeatTimeoutMillis = 30000;
  private int checkpointIntervalMillis = 30000;
  private final int operatorMaxAttemptCount = 5;

  protected final  Map<String, String> containerStopRequests = new ConcurrentHashMap<String, String>();
  protected final  ConcurrentLinkedQueue<ContainerStartRequest> containerStartRequests = new ConcurrentLinkedQueue<ContainerStartRequest>();
  protected String shutdownDiagnosticsMessage = "";
  protected boolean forcedShutdown = false;

  private final Map<String, StramChildAgent> containers = new ConcurrentHashMap<String, StramChildAgent>();
  private final PhysicalPlan plan;
  private final String checkpointFsPath;
  private final List<Pair<PTOperator, Long>> purgeCheckpoints = new ArrayList<Pair<PTOperator, Long>>();
  private final Map<InetSocketAddress, BufferServerClient> bufferServers = new HashMap<InetSocketAddress, BufferServerClient>();


  public StreamingContainerManager(DAG dag) {
    this.plan = new PhysicalPlan(dag);

    this.windowSizeMillis = dag.getConf().getInt(DAG.STRAM_WINDOW_SIZE_MILLIS, 500);
    // try to align to it pleases eyes.
    windowStartMillis -= (windowStartMillis % 1000);
    checkpointFsPath = dag.getConf().get(DAG.STRAM_CHECKPOINT_DIR, "stram/" + System.currentTimeMillis() + "/checkpoints");
    this.checkpointIntervalMillis = dag.getConf().getInt(DAG.STRAM_CHECKPOINT_INTERVAL_MILLIS, this.checkpointIntervalMillis);

    AtomicInteger startupCountDown = new AtomicInteger(plan.getContainers().size());
    // request initial containers
    for (PTContainer container : plan.getContainers()) {
      // operators can deploy only after all containers are running (and buffer servers listen)
      this.containerStartRequests.add(new ContainerStartRequest(container, startupCountDown, startupCountDown));
    }
  }

  public int getNumRequiredContainers()
  {
    return containerStartRequests.size();
  }

  protected PhysicalPlan getPhysicalPlan() {
    return plan;
  }

  /**
   * Check periodically that child containers phone home
   *
   */
  public void monitorHeartbeat() {
    long currentTms = System.currentTimeMillis();
    for (Map.Entry<String,StramChildAgent> cse : containers.entrySet()) {
       String containerId = cse.getKey();
       StramChildAgent cs = cse.getValue();
       if (!cs.isComplete && cs.lastHeartbeatMillis + heartbeatTimeoutMillis < currentTms) {
         // TODO: separate startup timeout handling
         if (cs.createdMillis + heartbeatTimeoutMillis < currentTms) {
           // issue stop as process may still be hanging around (would have been detected by Yarn otherwise)
           LOG.info("Container {}@{} heartbeat timeout.", containerId, cse.getValue().container.host);
           containerStopRequests.put(containerId, containerId);
         }
       }
    }
    updateCheckpoints();
  }

  /**
   * Schedule container restart. Called by Stram after a failed container is
   * reported by the RM, or after heartbeat timeout occurs. <br>
   * Recovery will resolve affected operators (within the container and
   * everything downstream with respective recovery checkpoint states). Affected
   * operators will be undeployed, buffer server connections reset prior to
   * redeploy to recovery checkpoint.
   *
   * @param containerId
   */
  public void scheduleContainerRestart(String containerId) {

    StramChildAgent cs = getContainerAgent(containerId);
    if (cs.shutdownRequested == true) {
      return;
    }

    LOG.info("Initiating recovery for container {}@{}", containerId, cs.container.host);
    // building the checkpoint dependency,
    // downstream operators will appear first in map
    LinkedHashSet<PTOperator> checkpoints = new LinkedHashSet<PTOperator>();
    for (PTOperator node : cs.container.operators) {
      // TODO: traversal needs to include inline upstream operators
      updateRecoveryCheckpoints(node, checkpoints);
    }

    Map<PTContainer, List<PTOperator>> resetNodes = new HashMap<PTContainer, List<PhysicalPlan.PTOperator>>();
    // group by container
    for (PTOperator node : checkpoints) {
        List<PTOperator> nodes = resetNodes.get(node.container);
        if (nodes == null) {
          nodes = new ArrayList<PhysicalPlan.PTOperator>();
          resetNodes.put(node.container, nodes);
        }
        nodes.add(node);
    }

    // stop affected downstream operators (all except failed container)
    // particular order does not matter, remove all affected operators in each container in one sweep
    AtomicInteger undeployAckCountdown = new AtomicInteger();
    for (Map.Entry<PTContainer, List<PTOperator>> e : resetNodes.entrySet()) {
      if (e.getKey() != cs.container) {
        UndeployRequest r = new UndeployRequest(e.getKey(), undeployAckCountdown, null);
        r.setNodes(e.getValue());
        undeployAckCountdown.incrementAndGet();
        StramChildAgent downstreamContainer = getContainerAgent(e.getKey().containerId);
        downstreamContainer.addRequest(r);
      }
    }

    // deploy replacement container, depends on above downstream operators stop
    AtomicInteger failedContainerDeployCnt = new AtomicInteger(1);
    undeployAckCountdown.incrementAndGet(); // deploy waits for container start
    ContainerStartRequest dr = new ContainerStartRequest(cs.container, failedContainerDeployCnt, undeployAckCountdown);
    // launch replacement container, deploy request will be queued with new container agent in assignContainer
    containerStartRequests.add(dr);

    // (re)deploy affected operators
    // this can happen in parallel after buffer server state for recovered publishers is reset
    AtomicInteger redeployAckCountdown = new AtomicInteger();
    for (Map.Entry<PTContainer, List<PTOperator>> e : resetNodes.entrySet()) {
      // to reset publishers, clean buffer server past checkpoint so subscribers don't read stale data (including end of stream)
      for (PTOperator operator : e.getValue()) {
        for (PTOutput out : operator.outputs) {
          if (operator.container == cs.container) {
            LOG.debug("Skipping purge for buffer server in failed container {}", cs.container.containerId);
            continue;
          }
          final StreamDecl streamDecl = out.logicalStream;
          if (!(streamDecl.isInline() && out.isDownStreamInline())) {
            // following needs to match the concat logic in StramChild
            String sourceIdentifier = operator.id.concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(out.portName);
            // TODO: find way to mock this when testing rest of logic
            if (operator.container.bufferServerAddress.getPort() != 0) {
              BufferServerClient bsc = getBufferServerClient(operator);
              // reset publisher (stale operator may still write data until disconnected)
              // ensures new subscriber starting to read from checkpoint will wait until publisher redeploy cycle is complete
              bsc.reset(sourceIdentifier, 0);
            }
          }
        }
      }
      if (e.getKey() != cs.container) {
        DeployRequest r = new DeployRequest(redeployAckCountdown, failedContainerDeployCnt);
        r.setNodes(e.getValue());
        redeployAckCountdown.incrementAndGet();
        StramChildAgent downstreamContainer = getContainerAgent(e.getKey().containerId);
        downstreamContainer.addRequest(r);
      }
    }

  }

  public void markComplete(String containerId) {
    StramChildAgent cs = containers.get(containerId);
    if (cs == null) {
      LOG.warn("Completion status for unknown container {}", containerId);
      return;
    }
    cs.isComplete = true;
  }


  public StramChildAgent assignContainerForTest(String containerId, InetSocketAddress bufferServerAddress)
  {
    for (PTContainer container : this.plan.getContainers()) {
      if (container.containerId == null) {
        container.containerId = containerId;
        container.bufferServerAddress = bufferServerAddress;
        StreamingContainerContext scc = newStreamingContainerContext();
        StramChildAgent ca = new StramChildAgent(container, scc);
        containers.put(containerId, new StramChildAgent(container, scc));
        return ca;
      }
    }
    throw new IllegalStateException("There are no more containers to deploy.");
  }

  /**
   * Get operators/streams for next container. Multiple operators can share a container.
   *
   * @param containerId
   * @param cdr
   */
  public void assignContainer(ContainerStartRequest cdr, String containerId, String containerHost, InetSocketAddress bufferServerAddr) {
    PTContainer container = cdr.container;
    if (container.containerId != null) {
      LOG.info("Removing existing container agent {}", cdr.container.containerId);
      this.containers.remove(container.containerId);
    }
    container.containerId = containerId;
    container.host = containerHost;
    container.bufferServerAddress = bufferServerAddr;

    StramChildAgent sca = new StramChildAgent(container, newStreamingContainerContext());
    containers.put(containerId, sca);

    // first entry to count down container start(s), signals ready for operator deployment
    DeployRequest initReq = new DeployRequest(cdr.executeWhenZero, null);
    sca.addRequest(initReq);

    DeployRequest deployRequest = new DeployRequest(cdr.ackCountdown, cdr.executeWhenZero);
    deployRequest.setNodes(container.operators);
    sca.addRequest(deployRequest);
  }

  private StreamingContainerContext newStreamingContainerContext() {
    StreamingContainerContext scc = new StreamingContainerContext();
    scc.setWindowSizeMillis(this.windowSizeMillis);
    scc.setStartWindowMillis(this.windowStartMillis);
    scc.setCheckpointDfsPath(this.checkpointFsPath);
    return scc;
  }


  public StramChildAgent getContainerAgent(String containerId) {
    StramChildAgent cs = containers.get(containerId);
    if (cs == null) {
      throw new AssertionError("Unknown container " + containerId);
    }
    return cs;
  }

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

    if (sca.container.bufferServerAddress == null) {
      // capture dynamically assigned address from container
      if (heartbeat.bufferServerHost != null) {
        sca.container.bufferServerAddress = InetSocketAddress.createUnresolved(heartbeat.bufferServerHost, heartbeat.bufferServerPort);
        LOG.info("Container {} buffer server: {}", sca.container.containerId, sca.container.bufferServerAddress);
      }
    }

    Map<String, OperatorStatus> statusMap = sca.operators;
    long lastHeartbeatIntervalMillis = currentTimeMillis - sca.lastHeartbeatMillis;

    for (StreamingNodeHeartbeat shb : heartbeat.getDnodeEntries()) {

      OperatorStatus status = statusMap.get(shb.getNodeId());
      if (status == null) {
        LOG.error("Heartbeat for unknown operator {} (container {})", shb.getNodeId(), heartbeat.getContainerId());
        continue;
      }

      //ReflectionToStringBuilder b = new ReflectionToStringBuilder(shb);
      //LOG.info("node {} ({}) heartbeat: {}, totalTuples: {}, totalBytes: {} - {}",
      //         new Object[]{shb.getNodeId(), status.node.getLogicalId(), b.toString(), status.tuplesTotal, status.bytesTotal, heartbeat.getContainerId()});

      StreamingNodeHeartbeat previousHeartbeat = status.lastHeartbeat;
      status.lastHeartbeat = shb;

      if (shb.getState().compareTo(DNodeState.FAILED.name()) == 0) {
        // count failure transitions *->FAILED, applies to initialization as well as intermittent failures
        if (previousHeartbeat == null || DNodeState.FAILED.name().compareTo(previousHeartbeat.getState()) != 0) {
          status.operator.failureCount++;
          LOG.warn("Operator failure: {} count: {}", status.operator, status.operator.failureCount);
          Integer maxAttempts = status.operator.logicalNode.getAttributes().attrValue(OperatorContext.RECOVERY_ATTEMPTS, this.operatorMaxAttemptCount);
          if (status.operator.failureCount <= maxAttempts) {
            // restart entire container in attempt to recover operator
            // in the future a more sophisticated recovery strategy could
            // involve initial redeploy attempt(s) of affected operator in
            // existing container or sandbox container for just the operator
            LOG.error("Issuing container stop to restart after operator failure {}", status.operator);
            containerStopRequests.put(sca.container.containerId, sca.container.containerId);
          } else {
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
        List<OperatorStats> statsList = shb.getWindowStats();
        for (OperatorStats stats : statsList) {
          Collection<PortStats> ports = stats.inputPorts;
          if (ports != null) {
            for (PortStats s: ports) {
              tuplesProcessed += s.processedCount;
            }
          }

          ports = stats.ouputPorts;
          if (ports != null) {
            for (PortStats s: ports) {
              tuplesEmitted += s.processedCount;
            }
          }
          status.currentWindowId = stats.windowId;
        }

        status.totalTuplesProcessed += tuplesProcessed;
        status.totalTuplesEmitted += tuplesEmitted;
        if (lastHeartbeatIntervalMillis > 0) {
          status.tuplesProcessedPSMA10.add((tuplesProcessed*1000)/lastHeartbeatIntervalMillis);
          status.tuplesEmittedPSMA10.add((tuplesEmitted*1000)/lastHeartbeatIntervalMillis);
        }

        // checkpoint tracking
        PTOperator node = status.operator;
        if (shb.getLastBackupWindowId() != 0) {
          synchronized (node.checkpointWindows) {
            if (!node.checkpointWindows.isEmpty()) {
              Long lastCheckpoint = node.checkpointWindows.getLast();
              // no need for extra work unless checkpoint moves
              if (lastCheckpoint.longValue() != shb.getLastBackupWindowId()) {
                node.checkpointWindows.add(shb.getLastBackupWindowId());
                //System.out.println(node.container.containerId + " " + node + " checkpoint " + Codec.getStringWindowId(shb.getLastBackupWindowId()) + " at " + Codec.getStringWindowId(currentTimeMillis/1000));
              }
            } else {
              node.checkpointWindows.add(shb.getLastBackupWindowId());
            }
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
    } else {
      if (sca.shutdownRequested) {
        LOG.info("requesting shutdown for container {}", heartbeat.getContainerId());
        rsp.shutdown = true;
      }
    }

    List<StramToNodeRequest> requests = new ArrayList<StramToNodeRequest>();
    if (checkpointIntervalMillis > 0) {
      if (sca.lastCheckpointRequestMillis + checkpointIntervalMillis < currentTimeMillis) {
        //System.out.println("\n\n*** sending checkpoint to " + cs.container.containerId + " at " + currentTimeMillis);
        for (OperatorStatus os : sca.operators.values()) {
          if (os.lastHeartbeat != null && os.lastHeartbeat.getState().compareTo(DNodeState.ACTIVE.name()) == 0) {
            StramToNodeRequest backupRequest = new StramToNodeRequest();
            backupRequest.setNodeId(os.operator.id);
            backupRequest.setRequestType(RequestType.CHECKPOINT);
            backupRequest.setRecoveryCheckpoint(os.operator.recoveryCheckpoint);
            requests.add(backupRequest);
          }
        }
        sca.lastCheckpointRequestMillis = currentTimeMillis;
      }
    }
    rsp.nodeRequests = requests;
    return rsp;
  }

  private boolean isApplicationIdle()
  {
    for (StramChildAgent csa : this.containers.values()) {
      if (!csa.isIdle()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compute checkpoints required for a given operator instance to be recovered.
   * This is done by looking at checkpoints available for downstream dependencies first,
   * and then selecting the most recent available checkpoint that is smaller than downstream.
   * @param operator Operator instance for which to find recovery checkpoint
   * @param visited Set into which to collect visited dependencies
   * @return Checkpoint that can be used to recover (along with dependencies in visitedCheckpoints).
   */
  public long updateRecoveryCheckpoints(PTOperator operator, Set<PTOperator> visited) {
    long maxCheckpoint = operator.getRecentCheckpoint();
    // find smallest most recent subscriber checkpoint
    for (PTOutput out : operator.outputs) {
      for (DAG.InputPortMeta targetPort : out.logicalStream.getSinks()) {
        OperatorWrapper lDownNode = targetPort.getOperatorWrapper();
        if (lDownNode != null) {
          List<PTOperator> downNodes = plan.getOperators(lDownNode);
          for (PTOperator downNode : downNodes) {
            if (!visited.contains(downNode)) {
              // downstream traversal
              updateRecoveryCheckpoints(downNode, visited);
            }
            maxCheckpoint = Math.min(maxCheckpoint, downNode.recoveryCheckpoint);
          }
        }
      }
    }
    // find checkpoint for downstream dependency, remove previous checkpoints
    long c1 = 0;
    synchronized (operator.checkpointWindows) {
      if (operator.checkpointWindows != null && !operator.checkpointWindows.isEmpty()) {
        if ((c1 = operator.checkpointWindows.getFirst().longValue()) <= maxCheckpoint) {
          long c2 = 0;
          while (operator.checkpointWindows.size() > 1 && (c2 = operator.checkpointWindows.get(1).longValue()) <= maxCheckpoint) {
            operator.checkpointWindows.removeFirst();
            LOG.debug("Checkpoint to purge: operator={} windowId={}", operator.id,  c1);
            this.purgeCheckpoints.add(new Pair<PTOperator, Long>(operator, c1));
            c1 = c2;
          }
        } else {
          c1 = 0;
        }
      }
    }
    visited.add(operator);
    return operator.recoveryCheckpoint = c1;
  }

  /**
   * Visit all operators to update current checkpoint.
   * Collect checkpoints that may be purged based on updated downstream state.
   * @param op
   * @param recentCheckpoint
   */
  private void updateCheckpoints() {
    Set<PTOperator> visitedCheckpoints = new LinkedHashSet<PTOperator>();
    for (OperatorWrapper logicalOperator : plan.getRootOperators()) {
      List<PTOperator> operators = plan.getOperators(logicalOperator);
      if (operators != null) {
        for (PTOperator operator : operators) {
          updateRecoveryCheckpoints(operator, visitedCheckpoints);
        }
      }
    }
    purgeCheckpoints();
  }

  private BufferServerClient getBufferServerClient(PTOperator operator) {
    BufferServerClient bsc = bufferServers.get(operator.container.bufferServerAddress);
    if (bsc == null) {
      bsc = new BufferServerClient(operator.container.bufferServerAddress);
      // use original address address as key
      bufferServers.put(operator.container.bufferServerAddress, bsc);
      LOG.debug("Added new buffer server client: " + operator.container.bufferServerAddress);
    }
    return bsc;
  }

  private void purgeCheckpoints() {
    BackupAgent ba = new HdfsBackupAgent(new Configuration(), checkpointFsPath);
    for (Pair<PTOperator, Long> p : purgeCheckpoints) {
      PTOperator operator = p.getFirst();
      try {
        ba.delete(operator.id, p.getSecond());
      } catch (Exception e) {
        LOG.error("Failed to purge checkpoint " + p, e);
      }
      // purge stream state when using buffer server
      for (PTOutput out : operator.outputs) {
        final StreamDecl streamDecl = out.logicalStream;
        if (!(streamDecl.isInline() && out.isDownStreamInline())) {
          // following needs to match the concat logic in StramChild
          String sourceIdentifier = operator.id.concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(out.portName);
          // purge everything from buffer server prior to new checkpoint
          BufferServerClient bsc = getBufferServerClient(operator);
          try {
            bsc.purge(sourceIdentifier, operator.checkpointWindows.getFirst()-1);
          } catch (Throwable  t) {
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
   */
  public void shutdownAllContainers(String message) {
    this.shutdownDiagnosticsMessage = message;
    LOG.info("Initiating application shutdown: " + message);
    for (StramChildAgent cs : this.containers.values()) {
      cs.shutdownRequested = true;
    }
  }

  public ArrayList<OperatorInfo> getNodeInfoList() {
    ArrayList<OperatorInfo> nodeInfoList = new ArrayList<OperatorInfo>();
    for (StramChildAgent container : this.containers.values()) {
      for (OperatorStatus os : container.operators.values()) {
        OperatorInfo ni = new OperatorInfo();
        ni.container = os.container.containerId + "@" + os.container.host;
        ni.id = os.operator.id;
        ni.name = os.operator.getLogicalId();
        StreamingNodeHeartbeat hb = os.lastHeartbeat;
        if (hb != null) {
          // initial heartbeat not yet received
          ni.status = hb.getState();
          ni.totalTuplesProcessed = os.totalTuplesProcessed;
          ni.totalTuplesEmitted = os.totalTuplesEmitted;
          ni.tuplesProcessedPSMA10 = os.tuplesProcessedPSMA10.getAvg();
          ni.tuplesEmittedPSMA10 = os.tuplesEmittedPSMA10.getAvg();
          ni.lastHeartbeat = os.lastHeartbeat.getGeneratedTms();
          ni.failureCount = os.operator.failureCount;
          ni.recoveryWindowId = os.operator.recoveryCheckpoint & 0xFFFF;
          ni.currentWindowId = os.currentWindowId & 0xFFFF;
        } else {
          // TODO: proper node status tracking
          StramChildAgent cs = containers.get(os.container.containerId);
          if (cs != null) {
            ni.status = cs.isComplete ? "CONTAINER_COMPLETE" : "CONTAINER_NEW";
          }
        }
        nodeInfoList.add(ni);
      }
    }
    return nodeInfoList;
  }

}

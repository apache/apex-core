/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;


import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import com.malhartech.api.OperatorSerDe;
import com.malhartech.stram.OperatorDeployInfo.InputDeployInfo;
import com.malhartech.stram.OperatorDeployInfo.OutputDeployInfo;
import com.malhartech.stram.PhysicalPlan.PTComponent;
import com.malhartech.stram.PhysicalPlan.PTContainer;
import com.malhartech.stram.PhysicalPlan.PTInput;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.PhysicalPlan.PTOutput;
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
  private final OperatorSerDe nodeSerDe = StramUtils.getNodeSerDe(null);

  protected final  Map<String, String> containerStopRequests = new ConcurrentHashMap<String, String>();
  protected final  ConcurrentLinkedQueue<DeployRequest> containerStartRequests = new ConcurrentLinkedQueue<DeployRequest>();
  protected String shutdownDiagnosticsMessage = "";

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

    // fill initial deploy requests
    for (PTContainer container : plan.getContainers()) {
      this.containerStartRequests.add(new DeployRequest(container, null));
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
           LOG.info("Triggering restart for container {} after heartbeat timeout.", containerId);
           containerStopRequests.put(containerId, containerId);
         }
       }
    }
    updateCheckpoints();
  }

  /**
   * Schedule container restart. Resolves downstream dependencies and checkpoint states.
   * Called by Stram after a failed container is reported by the RM,
   * or after heartbeat timeout occurs.
   * @param containerId
   */
  public void scheduleContainerRestart(String containerId) {
    LOG.info("Initiating recovery for container {}", containerId);

    StramChildAgent cs = getContainerAgent(containerId);

    // building the checkpoint dependency,
    // downstream operators will appear first in map
    // TODO: traversal needs to include inline upstream operators
    Map<PTOperator, Long> checkpoints = new LinkedHashMap<PTOperator, Long>();
    for (PTOperator node : cs.container.operators) {
      updateRecoveryCheckpoints(node, checkpoints);
    }

    Map<PTContainer, List<PTOperator>> resetNodes = new HashMap<PTContainer, List<PhysicalPlan.PTOperator>>();
    // group by container
    for (PTOperator node : checkpoints.keySet()) {
        List<PTOperator> nodes = resetNodes.get(node.container);
        if (nodes == null) {
          nodes = new ArrayList<PhysicalPlan.PTOperator>();
          resetNodes.put(node.container, nodes);
        }
        nodes.add(node);
    }

    // stop affected downstream operators (all except failed container)
    AtomicInteger undeployAckCountdown = new AtomicInteger();
    for (Map.Entry<PTContainer, List<PTOperator>> e : resetNodes.entrySet()) {
      if (e.getKey() != cs.container) {
        StreamingContainerContext ctx = createStramChildInitContext(e.getValue(), e.getKey(), checkpoints);
        UndeployRequest r = new UndeployRequest(e.getKey(), undeployAckCountdown, null);
        r.setNodes(ctx.nodeList);
        undeployAckCountdown.incrementAndGet();
        StramChildAgent downstreamContainer = getContainerAgent(e.getKey().containerId);
        downstreamContainer.addRequest(r);
      }
    }

    // schedule deployment for replacement container, depends on above downstream operators stop
    AtomicInteger failedContainerDeployCnt = new AtomicInteger(1);
    DeployRequest dr = new DeployRequest(cs.container, failedContainerDeployCnt, undeployAckCountdown);
    dr.checkpoints = checkpoints;
    // launch replacement container, deploy request will be queued with new container agent in assignContainer
    containerStartRequests.add(dr);

    // (re)deploy affected downstream operators
    AtomicInteger redeployAckCountdown = new AtomicInteger();
    for (Map.Entry<PTContainer, List<PTOperator>> e : resetNodes.entrySet()) {
      if (e.getKey() != cs.container) {
        StreamingContainerContext ctx = createStramChildInitContext(e.getValue(), e.getKey(), checkpoints);
        DeployRequest r = new DeployRequest(e.getKey(), redeployAckCountdown, failedContainerDeployCnt);
        r.setNodes(ctx.nodeList);
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

  /**
   * Create deploy info for logical node.<p>
   * <br>
   * @param dnodeId
   * @param nodeDecl
   * @return {@link com.malhartech.stram.OperatorDeployInfo}
   *
   */
  private OperatorDeployInfo createModuleDeployInfo(String dnodeId, OperatorWrapper operator)
  {
    OperatorDeployInfo ndi = new OperatorDeployInfo();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      // populate custom properties
      //Module node = StramUtils.initNode(nodeDecl.getNodeClass(), dnodeId, nodeDecl.getProperties());
      this.nodeSerDe.write(operator.getOperator(), os);
      ndi.serializedNode = os.toByteArray();
      os.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize " + operator + "(" + operator.getOperator().getClass() + ")", e);
    }
//    ndi.properties = operator.getnodeDecl.getProperties();
    ndi.declaredId = operator.getId();
    ndi.id = dnodeId;
    return ndi;
  }

  public StreamingContainerContext assignContainerForTest(String containerId, InetSocketAddress bufferServerAddress)
  {
    for (PTContainer container : this.plan.getContainers()) {
      if (container.containerId == null) {
        container.containerId = containerId;
        container.bufferServerAddress = bufferServerAddress;
        StreamingContainerContext scc = createStramChildInitContext(container.operators, container, Collections.<PTOperator, Long>emptyMap());
        containers.put(containerId, new StramChildAgent(container, scc));
        return scc;
      }
    }
    throw new IllegalStateException("There are no more containers to deploy.");
  }

  /**
   * Get operators/streams for next container. Multiple operators can share a container.
   *
   * @param containerId
   * @param bufferServerAddress Buffer server for publishers on the container.
   * @param cdr
   */
  public void assignContainer(DeployRequest cdr, String containerId, String containerHost, InetSocketAddress bufferServerAddress) {
    PTContainer container = cdr.container;
    if (container.containerId != null) {
      LOG.info("Removing existing container agent {}", cdr.container.containerId);
      this.containers.remove(container.containerId);
    } else {
      container.bufferServerAddress = bufferServerAddress;
    }
    container.containerId = containerId;
    container.host = containerHost;

    Map<PTOperator, Long> checkpoints = cdr.checkpoints;
    if (checkpoints == null) {
      checkpoints = Collections.emptyMap();
    }
    StreamingContainerContext initCtx = createStramChildInitContext(container.operators, container, checkpoints);
    cdr.setNodes(initCtx.nodeList);
    initCtx.nodeList = new ArrayList<OperatorDeployInfo>(0);

    StramChildAgent sca = new StramChildAgent(container, initCtx);
    containers.put(containerId, sca);
    sca.addRequest(cdr);
  }

  /**
   * Create the protocol mandated node/stream info for bootstrapping StramChild.
   * @param container
   * @param deployNodes
   * @param checkpoints
   * @return StreamingContainerContext
   */
  private StreamingContainerContext createStramChildInitContext(List<PTOperator> deployNodes, PTContainer container, Map<PTOperator, Long> checkpoints) {
    StreamingContainerContext scc = new StreamingContainerContext();
    scc.setWindowSizeMillis(this.windowSizeMillis);
    scc.setStartWindowMillis(this.windowStartMillis);
    scc.setCheckpointDfsPath(this.checkpointFsPath);

//    List<StreamPConf> streams = new ArrayList<StreamPConf>();
    Map<OperatorDeployInfo, PTOperator> nodes = new LinkedHashMap<OperatorDeployInfo, PTOperator>();
    Map<String, OutputDeployInfo> publishers = new LinkedHashMap<String, OutputDeployInfo>();

    for (PTOperator node : deployNodes) {
      OperatorDeployInfo ndi = createModuleDeployInfo(node.id, node.getLogicalNode());
      Long checkpointWindowId = checkpoints.get(node);
      if (checkpointWindowId != null) {
        LOG.debug("Node {} has checkpoint state {}", node.id, checkpointWindowId);
        ndi.checkpointWindowId = checkpointWindowId;
      }
      nodes.put(ndi, node);
      ndi.inputs = new ArrayList<InputDeployInfo>(node.inputs.size());
      ndi.outputs = new ArrayList<OutputDeployInfo>(node.outputs.size());

      for (PTOutput out : node.outputs) {
        final StreamDecl streamDecl = out.logicalStream;
        // buffer server or inline publisher
        OutputDeployInfo portInfo = new OutputDeployInfo();
        portInfo.declaredStreamId = streamDecl.getId();
        portInfo.portName = out.portName;

        if (!(streamDecl.isInline() && this.plan.isDownStreamInline(out))) {
          portInfo.bufferServerHost = node.container.bufferServerAddress.getHostName();
          portInfo.bufferServerPort = node.container.bufferServerAddress.getPort();
          if (streamDecl.getSerDeClass() != null) {
            portInfo.serDeClassName = streamDecl.getSerDeClass().getName();
          }
        } else {
          // target set below
          //portInfo.inlineTargetNodeId = "-1subscriberInOtherContainer";
        }

        ndi.outputs.add(portInfo);
        publishers.put(node.id + "/" + streamDecl.getId(), portInfo);
      }
    }

    // after we know all publishers within container, determine subscribers

    for (Map.Entry<OperatorDeployInfo, PTOperator> nodeEntry : nodes.entrySet()) {
      OperatorDeployInfo ndi = nodeEntry.getKey();
      PTOperator node = nodeEntry.getValue();
      for (PTInput in : node.inputs) {
        final StreamDecl streamDecl = in.logicalStream;
        // input from other node(s) OR input adapter
        if (streamDecl.getSource() == null) {
          throw new IllegalStateException("source is null: " + in);
        }
        PTComponent sourceNode = in.source;

        InputDeployInfo inputInfo = new InputDeployInfo();
        inputInfo.declaredStreamId = streamDecl.getId();
        inputInfo.portName = in.portName;
        inputInfo.sourceNodeId = sourceNode.id;
        inputInfo.sourcePortName = in.logicalStream.getSource().getPortName();
        if (in.partition != null) {
          inputInfo.partitionKeys = Arrays.asList(in.partition);
        }

        if (streamDecl.isInline() && sourceNode.container == node.container) {
          // inline input (both operators in same container and inline hint set)
          OutputDeployInfo outputInfo = publishers.get(sourceNode.id + "/" + streamDecl.getId());
          if (outputInfo == null) {
            throw new IllegalStateException("Missing publisher for inline stream " + streamDecl);
          }
        } else {
          // buffer server input
          // FIXME: address to come from upstream output port, should be assigned first
          InetSocketAddress addr = in.source.container.bufferServerAddress;
          if (addr == null) {
            LOG.warn("upstream address not assigned: " + in.source);
            addr = container.bufferServerAddress;
          }
          inputInfo.bufferServerHost = addr.getHostName();
          inputInfo.bufferServerPort = addr.getPort();
          if (streamDecl.getSerDeClass() != null) {
            inputInfo.serDeClassName = streamDecl.getSerDeClass().getName();
          }
        }
        ndi.inputs.add(inputInfo);
      }
    }

    scc.nodeList = new ArrayList<OperatorDeployInfo>(nodes.keySet());

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
    Map<String, OperatorStatus> statusMap = sca.operators;

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
            this.shutdownDiagnosticsMessage = String.format("Shutting down application after reaching failure threshold for %s", status.operator);
            LOG.error(this.shutdownDiagnosticsMessage);
            // TODO: propagate exit code / shutdown message to master
            shutdownAllContainers();
          }
        }
      }

      if (!status.isIdle()) {
        containerIdle = false;
        status.bytesTotal += shb.getNumberBytesProcessed();
        status.tuplesTotal += shb.getNumberTuplesProcessed();

        // checkpoint tracking
        PTOperator node = status.operator;
        if (shb.getLastBackupWindowId() != 0 && shb.getState() == DNodeState.ACTIVE.name()) {
          synchronized (node.checkpointWindows) {
            if (!node.checkpointWindows.isEmpty()) {
              Long lastCheckpoint = node.checkpointWindows.getLast();
              // no need for extra work unless checkpoint moves
              if (lastCheckpoint.longValue() != shb.getLastBackupWindowId()) {
                node.checkpointWindows.add(shb.getLastBackupWindowId());
                //System.out.println(node.container.containerId + " " + node + " checkpoint " + Long.toHexString(shb.getLastBackupWindowId()) + " at " + Long.toHexString(currentTimeMillis/1000));
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
        LOG.info("requesting idle shutdown for container {}", heartbeat.getContainerId());
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
   * @param visitedCheckpoints Map into which to collect checkpoints for visited dependencies
   * @return Checkpoint that can be used to recover (along with dependencies in visitedCheckpoints).
   */
  public long updateRecoveryCheckpoints(PTOperator operator, Map<PTOperator, Long> visitedCheckpoints) {
    long maxCheckpoint = operator.getRecentCheckpoint();
    // find smallest most recent subscriber checkpoint
    for (PTOutput out : operator.outputs) {
      for (DAG.InputPortMeta targetPort : out.logicalStream.getSinks()) {
        OperatorWrapper lDownNode = targetPort.getOperatorWrapper();
        if (lDownNode != null) {
          List<PTOperator> downNodes = plan.getOperators(lDownNode);
          for (PTOperator downNode : downNodes) {
            Long downstreamCheckpoint = visitedCheckpoints.get(downNode);
            if (downstreamCheckpoint == null) {
              // downstream traversal
              downstreamCheckpoint = updateRecoveryCheckpoints(downNode, visitedCheckpoints);
            }
            maxCheckpoint = Math.min(maxCheckpoint, downstreamCheckpoint);
          }
        }
      }
    }
    // find checkpoint for downstream dependency, remove checkpoints that are no longer needed
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
    visitedCheckpoints.put(operator, c1);
    return c1;
  }

  /**
   * Visit all operators to update current checkpoint.
   * Collect checkpoints that may be purged based on updated downstream state.
   * @param op
   * @param recentCheckpoint
   */
  private void updateCheckpoints() {
    Map<PTOperator, Long> visitedCheckpoints = new LinkedHashMap<PTOperator, Long>();
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
        if (!(streamDecl.isInline() && this.plan.isDownStreamInline(out))) {
          // following needs to match the concat logic in StramChild
          String sourceIdentifier = operator.id.concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(out.portName);
          // purge everything from buffer server prior to new checkpoint
          BufferServerClient bsc = bufferServers.get(operator.container.bufferServerAddress);
          if (bsc == null) {
            bsc = new BufferServerClient(operator.container.bufferServerAddress);
            bufferServers.put(bsc.addr, bsc);
            LOG.debug("Added new buffer server client: " + operator.container.bufferServerAddress);
          }
          bsc.purge(sourceIdentifier, operator.checkpointWindows.getFirst()-1);
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
  public void shutdownAllContainers() {
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
          ni.totalBytes = os.bytesTotal;
          ni.totalTuples = os.tuplesTotal;
          ni.lastHeartbeat = os.lastHeartbeat.getGeneratedTms();
          ni.failureCount = os.operator.failureCount;
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

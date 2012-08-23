/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.StramChildAgent.DeployRequest;
import com.malhartech.stram.StramChildAgent.UndeployRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;
import com.malhartech.stram.TopologyDeployer.PTComponent;
import com.malhartech.stram.TopologyDeployer.PTContainer;
import com.malhartech.stram.TopologyDeployer.PTInput;
import com.malhartech.stram.TopologyDeployer.PTInputAdapter;
import com.malhartech.stram.TopologyDeployer.PTNode;
import com.malhartech.stram.TopologyDeployer.PTOutput;
import com.malhartech.stram.TopologyDeployer.PTOutputAdapter;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;
import com.malhartech.stram.webapp.NodeInfo;

/**
 * 
 * Tracks topology provisioning/allocation to containers<p>
 * <br>
 */
public class DNodeManager
{
  private final static Logger LOG = LoggerFactory.getLogger(DNodeManager.class);
  private long windowStartMillis = System.currentTimeMillis();
  private int windowSizeMillis = 500;
  private int heartbeatTimeoutMillis = 30000;
  private int checkpointIntervalMillis = 30000;
  
  private class NodeStatus
  {
    StreamingNodeHeartbeat lastHeartbeat;
    final PTComponent node;
    final PTContainer container;
    int tuplesTotal;
    int bytesTotal;
    
    private NodeStatus(PTContainer container, PTComponent node) {
      this.node = node;
      this.container = container;
    }

    boolean canShutdown()
    {
      // idle or output adapter
      if ((lastHeartbeat != null && DNodeState.IDLE.name().equals(lastHeartbeat.getState()))) {
        return true;
      }
      return (node instanceof PTOutputAdapter);
    }
  }
    
  final protected Map<String, String> containerStopRequests = new ConcurrentHashMap<String, String>();
  final protected ConcurrentLinkedQueue<DeployRequest> containerStartRequests = new ConcurrentLinkedQueue<DeployRequest>();
  final private Map<String, StramChildAgent> containers = new ConcurrentHashMap<String, StramChildAgent>();
  final private Map<String, NodeStatus> nodeStatusMap = new ConcurrentHashMap<String, NodeStatus>();
  final private TopologyDeployer deployer;
  final private String checkpointDir;
  
  public DNodeManager(TopologyBuilder topology) {
    this.deployer = new TopologyDeployer();
    this.deployer.init(topology.getContainerCount(), topology);

    this.windowSizeMillis = topology.getConf().getInt(TopologyBuilder.STRAM_WINDOW_SIZE_MILLIS, 500);
    // try to align to it pleases eyes.
    windowStartMillis -= (windowStartMillis % 1000);
    checkpointDir = topology.getConf().get(TopologyBuilder.STRAM_CHECKPOINT_DIR, "stram/" + System.currentTimeMillis() + "/checkpoints");
    this.checkpointIntervalMillis = topology.getConf().getInt(TopologyBuilder.STRAM_CHECKPOINT_INTERVAL_MILLIS, this.checkpointIntervalMillis);
    
    // fill initial deploy requests
    for (PTContainer container : deployer.getContainers()) {
      this.containerStartRequests.add(new DeployRequest(container, null));
    }
    
  }

  public int getNumRequiredContainers()
  {
    return containerStartRequests.size();
  }

  protected TopologyDeployer getTopologyDeployer() {
    return deployer;
  }
  
  /**
   * Check periodically that child containers phone home
   */
  public void monitorHeartbeat() {
    long currentTms = System.currentTimeMillis();
    for (Map.Entry<String,StramChildAgent> cse : containers.entrySet()) {
       String containerId = cse.getKey();
       StramChildAgent cs = cse.getValue();
       if (!cs.isComplete && cs.lastHeartbeatMillis + heartbeatTimeoutMillis < currentTms) {
         // TODO: separate startup timeout handling
         if (cs.createdMillis + heartbeatTimeoutMillis < currentTms) {
           // issue stop as probably process is still hanging around (would have been detected by Yarn otherwise)
           LOG.info("Triggering restart for container {} after heartbeat timeout.", containerId);
           containerStopRequests.put(containerId, containerId);
           restartContainer(containerId);
         }
       }
    }
  }

  /**
   * Schedule container restart. Resolves downstream dependencies and checkpoint states.
   * Called by Stram after a failed container is reported by the RM,
   * or after heartbeat timeout occurs.
   * @param containerId
   */
  public void restartContainer(String containerId) {
    LOG.info("Initiating recovery for container {}", containerId);

    StramChildAgent cs = getContainerAgent(containerId);

    // building the checkpoint dependency, 
    // downstream nodes will appear first in map
    Map<PTNode, Long> checkpoints = new LinkedHashMap<PTNode, Long>();
    for (PTNode node : cs.container.nodes) {
      getRecoveryCheckpoint(node, checkpoints);
    }

    Map<PTContainer, List<PTNode>> resetNodes = new HashMap<PTContainer, List<TopologyDeployer.PTNode>>();
    // group by container 
    for (PTNode node : checkpoints.keySet()) {
        List<PTNode> nodes = resetNodes.get(node.container);
        if (nodes == null) {
          nodes = new ArrayList<TopologyDeployer.PTNode>();
          resetNodes.put(node.container, nodes);
        }
        nodes.add(node);
    }

    // stop affected downstream dependency nodes (all except failed container)
    AtomicInteger undeployAckCountdown = new AtomicInteger();
    for (Map.Entry<PTContainer, List<PTNode>> e : resetNodes.entrySet()) {
      if (e.getKey() != cs.container) {
        StreamingContainerContext ctx = createStramChildInitContext(e.getValue(), e.getKey(), checkpoints);
        UndeployRequest r = new UndeployRequest(e.getKey(), undeployAckCountdown, null);
        r.setNodes(ctx.getNodes(), ctx.getStreams());
        undeployAckCountdown.incrementAndGet();
        StramChildAgent downstreamContainer = getContainerAgent(e.getKey().containerId);
        downstreamContainer.addRequest(r);
      }
    }
    
    // schedule deployment for replacement container, depends on above downstream nodes stop
    AtomicInteger failedContainerDeployCnt = new AtomicInteger(1);
    DeployRequest dr = new DeployRequest(cs.container, failedContainerDeployCnt, undeployAckCountdown);
    dr.checkpoints = checkpoints;
    // launch replacement container, the deploy request will be queued with new container agent in assignContainer
    containerStartRequests.add(dr); 
    
    // (re)deploy affected downstream nodes
    AtomicInteger redeployAckCountdown = new AtomicInteger();
    for (Map.Entry<PTContainer, List<PTNode>> e : resetNodes.entrySet()) {
      if (e.getKey() != cs.container) {
        
        // TODO: pass in the start window id
        
        StreamingContainerContext ctx = createStramChildInitContext(e.getValue(), e.getKey(), checkpoints);
        DeployRequest r = new DeployRequest(e.getKey(), redeployAckCountdown, failedContainerDeployCnt);
        r.setNodes(ctx.getNodes(), ctx.getStreams());
        redeployAckCountdown.incrementAndGet();
        StramChildAgent downstreamContainer = getContainerAgent(cs.container.containerId);
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
   * Create node tracking context for logical node. Exposed here for tests.
   *
   * @param dnodeId
   * @param nodeConf
   * @return
   */
  public static NodePConf createNodeContext(String dnodeId, NodeConf nodeConf)
  {
    NodePConf snc = new NodePConf();
    snc.setDnodeClassName(nodeConf.getProperties().get(TopologyBuilder.NODE_CLASSNAME));
    if (snc.getDnodeClassName() == null) {
      throw new IllegalArgumentException(String.format("Configuration for node '%s' is missing property '%s'", nodeConf.getId(), TopologyBuilder.NODE_CLASSNAME));
    }
    snc.setProperties(nodeConf.getProperties());
    snc.setLogicalId(nodeConf.getId());
    snc.setDnodeId(dnodeId);
    return snc;
  }
  
  private NodePConf newAdapterNodeContext(String id, StreamConf streamConf, Long checkpointWindowId)
  {
    NodePConf snc = new NodePConf();
    snc.setDnodeClassName(AdapterWrapperNode.class.getName());
    Map<String, String> properties = new HashMap<String, String>(streamConf.getProperties());
    String streamClassName = properties.get(TopologyBuilder.STREAM_CLASSNAME);
    if (streamClassName == null) {
      throw new IllegalArgumentException(String.format("Configuration for node '%s' is missing property '%s'", streamConf.getId(), TopologyBuilder.STREAM_CLASSNAME));
    }
    properties.put(AdapterWrapperNode.KEY_STREAM_CLASS_NAME, streamClassName);
    properties.put(AdapterWrapperNode.KEY_IS_INPUT, String.valueOf(streamConf.getSourceNode() == null));
    if (checkpointWindowId != null) {
      properties.put(AdapterWrapperNode.CHECKPOINT_WINDOW_ID, String.valueOf(checkpointWindowId.longValue()));
    }
    snc.setProperties(properties);
    snc.setLogicalId(streamConf.getId());
    snc.setDnodeId(id);
    return snc;
  }

  private StreamPConf newStreamContext(StreamConf streamConf, InetSocketAddress bufferServerAddress,
                                       byte[] subscriberPartition, String sourceNodeId, String targetNodeId)
  {
    // create new stream info and assign buffer server
    StreamPConf sc = new StreamPConf();
    sc.setId(streamConf.getId());
    sc.setBufferServerHost(bufferServerAddress.getHostName());
    sc.setBufferServerPort(bufferServerAddress.getPort());
    sc.setInline(streamConf.isInline());
    if (subscriberPartition != null) {
      sc.setPartitionKeys(Arrays.asList(subscriberPartition));
    }
    sc.setSourceNodeId(sourceNodeId);
    sc.setTargetNodeId(targetNodeId);
    return sc;
  }

  public StreamingContainerContext assignContainerForTest(String containerId, InetSocketAddress bufferServerAddress)
  {
    for (PTContainer container : this.deployer.getContainers()) {
      if (container.containerId == null) {
        container.containerId = containerId;
        container.bufferServerAddress = bufferServerAddress;
        StreamingContainerContext scc = createStramChildInitContext(container.nodes, container, Collections.<PTNode, Long>emptyMap());
        containers.put(containerId, new StramChildAgent(container, scc));
        return scc;
      }
    }
    throw new IllegalStateException("There are no more containers to deploy.");
  }

  /**
   * Get nodes/streams for next container. Multiple nodes can share a container.
   *
   * @param containerId
   * @param bufferServerAddress Buffer server for publishers on the container.
   * @return
   */
  public void assignContainer(DeployRequest cdr, String containerId, InetSocketAddress bufferServerAddress) {
    PTContainer container = cdr.container;
    if (container.containerId != null) {
      LOG.info("Removing existing container agent {}", cdr.container.containerId);
      this.containers.remove(container.containerId);
    } else {
      container.bufferServerAddress = bufferServerAddress;
    }
    container.containerId = containerId;

    Map<PTNode, Long> checkpoints = cdr.checkpoints;
    if (checkpoints == null) {
      checkpoints = Collections.emptyMap();
    }
    StreamingContainerContext initCtx = createStramChildInitContext(container.nodes, container, checkpoints);
    cdr.setNodes(initCtx.getNodes(), initCtx.getStreams());
    initCtx.setNodes(new ArrayList<NodePConf>(0));
    initCtx.setStreams(new ArrayList<StreamPConf>(0));
    
    StramChildAgent sca = new StramChildAgent(container, initCtx);
    containers.put(containerId, sca);
    sca.addRequest(cdr);
  }

  /**
   * Create the protocol mandated node/stream info for bootstrapping StramChild.
   * @param container
   * @return
   */
  private StreamingContainerContext createStramChildInitContext(List<PTNode> deployNodes, PTContainer container, Map<PTNode, Long> checkpoints) {
    StreamingContainerContext scc = new StreamingContainerContext();
    scc.setWindowSizeMillis(this.windowSizeMillis);
    scc.setStartWindowMillis(this.windowStartMillis);
    scc.setCheckpointDfsPath(this.checkpointDir);
    
    List<StreamPConf> streams = new ArrayList<StreamPConf>();
    Map<NodePConf, PTComponent> nodes = new LinkedHashMap<NodePConf, PTComponent>();
    Map<String, StreamPConf> publishers = new LinkedHashMap<String, StreamPConf>();
    
    for (PTNode node : deployNodes) {
      NodePConf pnodeConf = createNodeContext(node.id, node.getLogicalNode());
      Long checkpointWindowId = checkpoints.get(node);
      if (checkpointWindowId != null) {
        LOG.debug("Node {} has checkpoint state {}", node.id, checkpointWindowId);
        pnodeConf.setCheckpointWindowId(checkpointWindowId);
      }
      nodes.put(pnodeConf, node);
      
      for (PTOutput out : node.outputs) {
        final StreamConf streamConf = out.logicalStream;
        if (out instanceof PTOutputAdapter) {
          NodePConf adapterNode = newAdapterNodeContext(out.id, streamConf, checkpointWindowId);
          nodes.put(adapterNode, out);
          List<PTNode> upstreamNodes = deployer.getNodes(streamConf.getSourceNode());
          if (upstreamNodes.size() == 1) {
            // inline adapter and source node
            StreamPConf sc = newStreamContext(streamConf, node.container.bufferServerAddress, null, 
                node.id, adapterNode.getDnodeId());
            sc.setInline(true);
            sc.setProperties(streamConf.getProperties());
            streams.add(sc);
          } else {
            // partitioned source node - adapter subscribes to buffer server(s)
            for (PTNode upstreamNode : upstreamNodes) {
              // merge node
              StreamPConf sc = newStreamContext(streamConf, node.container.bufferServerAddress, null,
                  upstreamNode.id, adapterNode.getDnodeId());
              if (upstreamNode == node) {
                // adapter deployed to same container
                sc.setInline(true);
              } else {
                sc.setInline(false);
              }
              sc.setBufferServerChannelType(streamConf.getId());
              sc.setProperties(streamConf.getProperties());
              streams.add(sc);
            }
          }
        } else {
          // buffer server or inline publisher, intra container case handled below 
          StreamPConf sc = newStreamContext(streamConf, node.container.bufferServerAddress, null, node.id, "-1subscriberInOtherContainer");
          sc.setBufferServerChannelType(streamConf.getSourceNode().getId());
          sc.setProperties(streamConf.getProperties());
          publishers.put(node.id + "/" + streamConf.getId(), sc);
        }
      }
    }

    // after we know all publishers within container, determine subscribers
    
    for (PTNode subscriberNode : deployNodes) {
      for (PTInput in : subscriberNode.inputs) {
        final StreamConf streamConf = in.logicalStream;
        if (in instanceof PTInputAdapter) {
          // input adapter, with implementation class
          NodePConf adapterNode = newAdapterNodeContext(in.id, streamConf, checkpoints.get(subscriberNode));
          nodes.put(adapterNode, in);
          List<PTNode> subscriberNodes = deployer.getNodes(streamConf.getTargetNode());
          if (subscriberNodes.size() == 1) {
            // inline adapter and target node
            StreamPConf sc = newStreamContext(streamConf, in.getBufferServerAddress(), null,
                adapterNode.getDnodeId(), subscriberNode.id);
            sc.setInline(true);
            sc.setProperties(streamConf.getProperties());
            streams.add(sc);
          }
          else {
            // multiple target nodes - adapter publishes to buffer server
            StreamPConf sc = newStreamContext(streamConf, in.getBufferServerAddress(), in.partition, 
                adapterNode.getDnodeId(), subscriberNode.id);
            sc.setInline(false);
            // type is adapter name for multiple downstream nodes to be able to subscribe
            sc.setBufferServerChannelType(streamConf.getId());
            sc.setProperties(streamConf.getProperties());
            streams.add(sc);
          }
        } else {
          // input from other node(s) OR input adapter
          if (streamConf.getSourceNode() == null) {
            // source is input adapter
            StreamPConf sc = newStreamContext(streamConf, in.getBufferServerAddress(), in.partition,
                in.source.id, subscriberNode.id);
            sc.setBufferServerChannelType(streamConf.getId());
            sc.setProperties(streamConf.getProperties());
            streams.add(sc);
          } else {
            PTNode sourceNode = (PTNode)in.source;
            StreamPConf sc = newStreamContext(streamConf, in.getBufferServerAddress(), in.partition,
                in.source.id, subscriberNode.id);
            sc.setBufferServerChannelType(streamConf.getSourceNode().getId());
            sc.setProperties(streamConf.getProperties());
            sc.setInline(false);
            if (sourceNode.container == subscriberNode.container) {
              // connection within container
              publishers.remove(in.source.id + "/" + streamConf.getId());
              if (deployer.getNodes(streamConf.getSourceNode()).size() == 1 && streamConf.isInline()) {
                sc.setInline(true);
              }
            }
            streams.add(sc);
          }
        }
      }
    }

    // add remaining publishers (subscribers in other containers)
    streams.addAll(publishers.values());
    
    scc.setNodes(new ArrayList<NodePConf>(nodes.keySet()));
    scc.setStreams(streams);
    
    for (Map.Entry<NodePConf, PTComponent> e : nodes.entrySet()) {
      this.nodeStatusMap.put(e.getKey().getDnodeId(), new NodeStatus(container, e.getValue()));
    }
    
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
//addContainerStopRequest(heartbeat.getContainerId());    
    boolean containerIdle = true;
    long currentTimeMillis = System.currentTimeMillis();
    
    for (StreamingNodeHeartbeat shb : heartbeat.getDnodeEntries()) {

      NodeStatus status = nodeStatusMap.get(shb.getNodeId());
      if (status == null) {
        LOG.error("Heartbeat for unknown node {} (container {})", shb.getNodeId(), heartbeat.getContainerId());
        continue;
      }

      //ReflectionToStringBuilder b = new ReflectionToStringBuilder(shb);
      //LOG.info("node {} ({}) heartbeat: {}, totalTuples: {}, totalBytes: {} - {}",
      //         new Object[]{shb.getNodeId(), status.node.getLogicalId(), b.toString(), status.tuplesTotal, status.bytesTotal, heartbeat.getContainerId()});

      status.lastHeartbeat = shb;
      if (!status.canShutdown()) {
        containerIdle = false;
        status.bytesTotal += shb.getNumberBytesProcessed();
        status.tuplesTotal += shb.getNumberTuplesProcessed();
        checkNodeLoad(status, shb);
      }
    }

    StramChildAgent cs = getContainerAgent(heartbeat.getContainerId());
    cs.lastHeartbeatMillis = currentTimeMillis;
    LOG.info("Heartbeat container {}", heartbeat.getContainerId());
    
    ContainerHeartbeatResponse rsp = cs.pollRequest();
    if (rsp == null) {
      rsp = new ContainerHeartbeatResponse();
    }
    
    // below should be merged into pollRequest
    if (containerIdle && isApplicationIdle()) {
      LOG.info("requesting idle shutdown for container {}", heartbeat.getContainerId());
      rsp.setShutdown(true);
    } else {
      if (cs != null && cs.shutdownRequested) {
        LOG.info("requesting idle shutdown for container {}", heartbeat.getContainerId());
        rsp.setShutdown(true);
      }
    }
    
    List<StramToNodeRequest> requests = new ArrayList<StramToNodeRequest>();
    if (checkpointIntervalMillis > 0) {
      if (cs.lastCheckpointRequestMillis + checkpointIntervalMillis < currentTimeMillis) {
        for (PTNode node : cs.container.nodes) {
          StramToNodeRequest backupRequest = new StramToNodeRequest();
          backupRequest.setNodeId(node.id);
          backupRequest.setRequestType(RequestType.CHECKPOINT);
          requests.add(backupRequest);
        }
        cs.lastCheckpointRequestMillis = currentTimeMillis;
      }
    }
    rsp.setNodeRequests(requests);
    return rsp;
  }

  private boolean isApplicationIdle()
  {
    for (NodeStatus nodeStatus : this.nodeStatusMap.values()) {
      if (!nodeStatus.canShutdown()) {
        return false;
      }
    }
    return true;
  }

  private void checkNodeLoad(NodeStatus status, StreamingNodeHeartbeat shb)
  {
    if (!(status.node instanceof PTNode)) {
      LOG.warn("Cannot find the configuration for node {}", shb.getNodeId());
      return;
    }
    
    NodeConf nodeConf = ((PTNode)status.node).getLogicalNode();
    // check load constraints
    int tuplesProcessed = shb.getNumberTuplesProcessed();
    // TODO: populate into bean at initialization time
    Map<String, String> properties = nodeConf.getProperties();
    if (properties.containsKey(TopologyBuilder.NODE_LB_TUPLECOUNT_MIN)) {
      int minTuples = new Integer(properties.get(TopologyBuilder.NODE_LB_TUPLECOUNT_MIN));
      if (tuplesProcessed < minTuples) {
        LOG.warn("Node {} processed {} messages below configured min {}", new Object[]{shb.getNodeId(), tuplesProcessed, minTuples});
      }
    }
    if (properties.containsKey(TopologyBuilder.NODE_LB_TUPLECOUNT_MAX)) {
      int maxTuples = new Integer(properties.get(TopologyBuilder.NODE_LB_TUPLECOUNT_MAX));
      if (tuplesProcessed > maxTuples) {
        LOG.warn("Node {} processed {} messages and exceeds configured max {}", new Object[]{shb.getNodeId(), tuplesProcessed, maxTuples});
      }
    }

    // checkpoint tracking
    PTNode node = (PTNode)status.node;
    if (shb.getLastBackupWindowId() != 0) {
      synchronized (node.checkpointWindows) {
        if (!node.checkpointWindows.isEmpty()) {
          Long lastCheckpoint = node.checkpointWindows.get(node.checkpointWindows.size()-1);
          // no need to do any work unless checkpoint moves
          if (lastCheckpoint.longValue() != shb.getLastBackupWindowId()) {
            // keep track of current
            node.checkpointWindows.add(shb.getLastBackupWindowId());
            // TODO: purge older checkpoints, if no longer needed downstream
          }
        } else {
          node.checkpointWindows.add(shb.getLastBackupWindowId());
        }
      }
    }
    
  }

  /**
   * Compute checkpoint required for a given node to be recovered.
   * This is done by looking at checkpoints available for downstream dependencies first,
   * and then selecting the most recent available checkpoint that is smaller than downstream. 
   * @param node Node for which to find recovery checkpoint
   * @param recoveryCheckpoints Map to collect all downstream recovery checkpoints
   * @return Checkpoint that can be used to recover node (along with dependent nodes in recoveryCheckpoints).
   */
  public long getRecoveryCheckpoint(PTNode node, Map<PTNode, Long> recoveryCheckpoints) {
    long maxCheckpoint = node.getRecentCheckpoint();
    // find smallest most recent subscriber checkpoint
    for (PTOutput out : node.outputs) {
      NodeConf lDownNode = out.logicalStream.getTargetNode(); 
      if (lDownNode != null) {
        List<PTNode> downNodes = deployer.getNodes(lDownNode);
        for (PTNode downNode : downNodes) {
          Long downstreamCheckpoint = recoveryCheckpoints.get(downNode);
          if (downstreamCheckpoint == null) {
            // downstream traversal
            downstreamCheckpoint = getRecoveryCheckpoint(downNode, recoveryCheckpoints);
          }
          maxCheckpoint = Math.min(maxCheckpoint, downstreamCheckpoint);
        }
      }
    }
    // find most recent checkpoint for downstream dependency    
    long c1 = 0;
    synchronized (node.checkpointWindows) {
      if (node.checkpointWindows != null && !node.checkpointWindows.isEmpty()) {
        if ((c1 = node.checkpointWindows.getFirst().longValue()) <= maxCheckpoint) {
          long c2 = 0;
          while (node.checkpointWindows.size() > 1 && (c2 = node.checkpointWindows.get(1).longValue()) <= maxCheckpoint) {
            node.checkpointWindows.removeFirst();
            // TODO: async hdfs cleanup task
            LOG.warn("Checkpoint purging not implemented node={} windowId={}", node.id,  c1);
            c1 = c2;
          }
        } else {
          c1 = 0;
        }
      }
    }
    recoveryCheckpoints.put(node, c1);
    return c1;
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
  
  public void addContainerStopRequest(String containerId) {
    containerStopRequests.put(containerId, containerId);
  }
  
  public ArrayList<NodeInfo> getNodeInfoList() {
    ArrayList<NodeInfo> nodeInfoList = new ArrayList<NodeInfo>(this.nodeStatusMap.size());
    for (NodeStatus ns : this.nodeStatusMap.values()) {
      NodeInfo ni = new NodeInfo();
      ni.containerId = ns.container.containerId;
      ni.id = ns.node.id;
      ni.name = ns.node.getLogicalId();
      StreamingNodeHeartbeat hb = ns.lastHeartbeat;
      if (hb != null) {
        // initial heartbeat not yet received
        ni.status = hb.getState();
        ni.totalBytes = ns.bytesTotal;
        ni.totalTuples = ns.tuplesTotal;
        ni.lastHeartbeat = ns.lastHeartbeat.getGeneratedTms();
      } else {
        // TODO: proper node status tracking
        StramChildAgent cs = containers.get(ns.container.containerId);
        if (cs != null) {
          ni.status = cs.isComplete ? "CONTAINER_COMPLETE" : "CONTAINER_NEW";
        }
      }
      nodeInfoList.add(ni);
    }
    return nodeInfoList;
  }
  
}

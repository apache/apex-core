/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Tracks topology provisioning/allocation to containers.
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
  
  private class ContainerStatus
  {
    private ContainerStatus(StreamingContainerContext ctx, PTContainer container) {
      this.containerContext = ctx;
      this.container = container;
    }
    
    boolean shutdownRequested = false;
    boolean isComplete = false;
    StreamingContainerContext containerContext;
    long lastHeartbeatMillis = 0;
    long lastCheckpointRequestMillis = 0;
    long createdMillis = System.currentTimeMillis();
    final PTContainer container;
  }
  
  public class ContainerDeployRequest {
    final PTContainer container;
    public ContainerDeployRequest(PTContainer container) {
      this.container = container;
    }
  }
  
  final public Map<String, String> containerStopRequests = new ConcurrentHashMap<String, String>();
  final public ConcurrentLinkedQueue<ContainerDeployRequest> deployRequests = new ConcurrentLinkedQueue<ContainerDeployRequest>();
  final private Map<String, ContainerStatus> containers = new ConcurrentHashMap<String, ContainerStatus>();
  final private Map<String, NodeStatus> nodeStatusMap = new ConcurrentHashMap<String, NodeStatus>();
  final private TopologyDeployer deployer;
  final private String checkpointDir;
  
  public DNodeManager(TopologyBuilder topology) {
    this.deployer = new TopologyDeployer();
    this.deployer.init(topology.getContainerCount(), topology);
    // try to align to it pleases eyes.
    windowStartMillis -= (windowStartMillis % 1000);
    checkpointDir = topology.getConf().get(TopologyBuilder.STRAM_CHECKPOINT_DIR, "stram/" + System.currentTimeMillis() + "/checkpoints");
    
    // fill initial deploy requests
    for (PTContainer container : deployer.getContainers()) {
      this.deployRequests.add(new ContainerDeployRequest(container));
    }
    
  }

  public int getNumRequiredContainers()
  {
    return deployRequests.size();
  }

  /**
   * Check periodically that child containers phone home
   */
  public void monitorHeartbeat() {
    long currentTms = System.currentTimeMillis();
    for (Map.Entry<String,ContainerStatus> cse : containers.entrySet()) {
       String containerId = cse.getKey();
       ContainerStatus cs = cse.getValue();
       if (!cs.isComplete && cs.lastHeartbeatMillis + heartbeatTimeoutMillis < currentTms) {
         // TODO: separate startup timeout handling
         if (cs.createdMillis + heartbeatTimeoutMillis < currentTms) {
           // issue stop as probably process is still hanging around (would have been detected by Yarn otherwise)
           LOG.info("Triggering restart for container {} after heartbeat timeout.", containerId);
           containerStopRequests.put(containerId, containerId);
           deployRequests.add(new ContainerDeployRequest(cs.container));
         }
       }
    }
  }

  /**
   * Called by Stram when a failed to container is reported by the RM to request restart.
   * @param containerId
   */
  public void restartContainer(String containerId) {
    ContainerStatus cs = containers.get(containerId);
    if (cs == null) {
      LOG.warn("Restart request for unknown container {}", containerId);
      return;
    }
    deployRequests.add(new ContainerDeployRequest(cs.container));
  }

  public void markComplete(String containerId) {
    ContainerStatus cs = containers.get(containerId);
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
  
  private NodePConf newAdapterNodeContext(String id, StreamConf streamConf)
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

  /**
   * Get nodes/streams for next container. Multiple nodes can share a container.
   *
   * @param containerId
   * @param bufferServerAddress Buffer server for publishers on the container.
   * @return
   */
  public synchronized StreamingContainerContext assignContainer(String containerId, InetSocketAddress bufferServerAddress)
  {
    for (PTContainer container : this.deployer.getContainers()) {
      if (container.containerId == null) {
        container.containerId = containerId;
        container.bufferServerAddress = bufferServerAddress;
        StreamingContainerContext scc = getStreamingContainerContext(container);
        containers.put(containerId, new ContainerStatus(scc, container));
        return scc;
      }
    }
    throw new IllegalStateException("There are no more containers to deploy.");
  }

  public void assignContainer(ContainerDeployRequest cdr, String containerId, InetSocketAddress bufferServerAddress) {
    PTContainer container = cdr.container;
    if (container.containerId != null) {
      LOG.info("Removing existing container status {}", cdr.container.containerId);
      this.containers.remove(container.containerId);
    } else {
      cdr.container.bufferServerAddress = bufferServerAddress;
    }
    container.containerId = containerId;
    StreamingContainerContext scc = getStreamingContainerContext(container);
    containers.put(containerId, new ContainerStatus(scc, container));
  }
  
  /**
   * Create the protocol mandated node/stream info for bootstrapping StramChild.
   * @param container
   * @return
   */
  private StreamingContainerContext getStreamingContainerContext(PTContainer container) {
    StreamingContainerContext scc = new StreamingContainerContext();
    scc.setWindowSizeMillis(this.windowSizeMillis);
    scc.setStartWindowMillis(this.windowStartMillis);
    scc.setCheckpointDfsPath(this.checkpointDir);
    
    List<StreamPConf> streams = new ArrayList<StreamPConf>();
    Map<NodePConf, PTComponent> nodes = new LinkedHashMap<NodePConf, PTComponent>();
    Map<String, StreamPConf> publishers = new LinkedHashMap<String, StreamPConf>();
    
    for (PTNode node : container.nodes) {
      NodePConf pnodeConf = createNodeContext(node.id, node.getLogicalNode());
      nodes.put(pnodeConf, node);
      
      for (PTOutput out : node.outputs) {
        final StreamConf streamConf = out.logicalStream;
        if (out instanceof PTOutputAdapter) {
          NodePConf adapterNode = newAdapterNodeContext(out.id, streamConf);
          nodes.put(adapterNode, out);
          List<PTNode> upstreamNodes = deployer.getNodes(streamConf.getSourceNode());
          if (upstreamNodes.size() == 1) {
            // inline adapter and source node
            StreamPConf sc = newStreamContext(streamConf, container.bufferServerAddress, null, 
                node.id, adapterNode.getDnodeId());
            sc.setInline(true);
            sc.setProperties(streamConf.getProperties());
            streams.add(sc);
          } else {
            // partitioned source node - adapter subscribes to buffer server(s)
            for (PTNode upstreamNode : upstreamNodes) {
              // merge node
              StreamPConf sc = newStreamContext(streamConf, container.bufferServerAddress, null,
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
          StreamPConf sc = newStreamContext(streamConf, container.bufferServerAddress, null, node.id, "-1subscriberInOtherContainer");
          sc.setBufferServerChannelType(streamConf.getSourceNode().getId());
          sc.setProperties(streamConf.getProperties());
          publishers.put(node.id + "/" + streamConf.getId(), sc);
        }
      }
    }

    // after we know all publishers within container, determine subscribers
    
    for (PTNode subscriberNode : container.nodes) {
      for (PTInput in : subscriberNode.inputs) {
        final StreamConf streamConf = in.logicalStream;
        if (in instanceof PTInputAdapter) {
          // input adapter, with implementation class
          NodePConf adapterNode = newAdapterNodeContext(in.id, streamConf);
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
  
  public StreamingContainerContext getContainerContext(String containerId)
  {
    ContainerStatus cs = containers.get(containerId);
    if (cs == null) {
      throw new IllegalArgumentException("No context for container " + containerId);
    }
    return cs.containerContext;
  }

  public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat heartbeat)
  {
//addContainerStopRequest(heartbeat.getContainerId());    
    boolean containerIdle = true;
    long currentTimeMillis = System.currentTimeMillis();
    
    for (StreamingNodeHeartbeat shb : heartbeat.getDnodeEntries()) {
      ReflectionToStringBuilder b = new ReflectionToStringBuilder(shb);

      NodeStatus status = nodeStatusMap.get(shb.getNodeId());
      if (status == null) {
        LOG.error("Heartbeat for unknown node {} (container {})", shb.getNodeId(), heartbeat.getContainerId());
        continue;
      }

      LOG.info("node {} ({}) heartbeat: {}, totalTuples: {}, totalBytes: {} - {}",
               new Object[]{shb.getNodeId(), status.node.getLogicalId(), b.toString(), status.tuplesTotal, status.bytesTotal, heartbeat.getContainerId()});

      status.lastHeartbeat = shb;
      if (!status.canShutdown()) {
        containerIdle = false;
        status.bytesTotal += shb.getNumberBytesProcessed();
        status.tuplesTotal += shb.getNumberTuplesProcessed();
        checkNodeLoad(status, shb);
      }
    }

    ContainerStatus cs = containers.get(heartbeat.getContainerId());
    cs.lastHeartbeatMillis = currentTimeMillis;
    
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
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
    if (cs.lastCheckpointRequestMillis + checkpointIntervalMillis < currentTimeMillis) {
      for (NodePConf nodeConf : cs.containerContext.getNodes()) {
        StramToNodeRequest backupRequest = new StramToNodeRequest();
        backupRequest.setNodeId(nodeConf.getDnodeId());
        backupRequest.setRequestType(RequestType.CHECKPOINT);
        requests.add(backupRequest);
      }
      cs.lastCheckpointRequestMillis = currentTimeMillis;
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

  }

  /**
   * Mark all containers for shutdown, next container heartbeat response
   * will propagate the shutdown request. This is controlled soft shutdown.
   * If containers don't respond, the application can be forcefully terminated
   * via yarn using forceKillApplication.
   */
  public void shutdownAllContainers() {
    for (ContainerStatus cs : this.containers.values()) {
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
        ContainerStatus cs = containers.get(ns.container.containerId);
        if (cs != null) {
          ni.status = cs.isComplete ? "CONTAINER_COMPLETE" : "CONTAINER_NEW";
        }
      }
      nodeInfoList.add(ni);
    }
    return nodeInfoList;
  }
  
}

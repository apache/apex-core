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

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
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
  
  private class NodeStatus
  {
    StreamingNodeHeartbeat lastHeartbeat;
    final PTComponent node;
    final String containerId;
    int tuplesTotal;
    int bytesTotal;
    
    private NodeStatus(String containerId, PTComponent node) {
      this.node = node;
      this.containerId = containerId;
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
    private ContainerStatus(StreamingContainerContext ctx) {
      this.containerContext = ctx;
    }
    
    boolean shutdownRequested = false;
    StreamingContainerContext containerContext;
  }
  
  public Map<String, String> containerStopRequests = new ConcurrentHashMap<String, String>();
  public Map<String, ContainerStatus> containers = new ConcurrentHashMap<String, ContainerStatus>();
  private Map<String, NodeStatus> nodeStatusMap = new ConcurrentHashMap<String, NodeStatus>();
  final private TopologyDeployer deployer;
  
  
  public DNodeManager(TopologyBuilder topology) {
    this.deployer = new TopologyDeployer();
    // TODO: get min/max containers from topology configuration
    this.deployer.init(topology.getContainerCount(), topology);
    // try to align to it pleases eyes.
    windowStartMillis -= (windowStartMillis % 1000);
  }

  public int getNumRequiredContainers()
  {
    return deployer.getContainers().size();
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
   * Assign streaming nodes to newly available container. Multiple nodes can run in a container.
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
        StreamingContainerContext scc = createStreamingContainerContext(container);
        containers.put(containerId, new ContainerStatus(scc));
        return scc;
      }
    }
    throw new IllegalStateException("There are no more containers to deploy.");
  }

  private StreamingContainerContext createStreamingContainerContext(PTContainer container) {
    StreamingContainerContext scc = new StreamingContainerContext();
    scc.setWindowSizeMillis(this.windowSizeMillis);
    scc.setStartWindowMillis(this.windowStartMillis);

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

    // add remaining publishers (with subscribers in other containers)
    streams.addAll(publishers.values());
    
    scc.setNodes(new ArrayList<NodePConf>(nodes.keySet()));
    scc.setStreams(streams);
    
    for (Map.Entry<NodePConf, PTComponent> e : nodes.entrySet()) {
      this.nodeStatusMap.put(e.getKey().getDnodeId(), new NodeStatus(container.containerId, e.getValue()));
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

    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    if (containerIdle && isApplicationIdle()) {
      LOG.info("requesting idle shutdown for container {}", heartbeat.getContainerId());
      rsp.setShutdown(true);
    } else {
      ContainerStatus cs = containers.get(heartbeat.getContainerId());
      if (cs != null && cs.shutdownRequested) {
        LOG.info("requesting idle shutdown for container {}", heartbeat.getContainerId());
        rsp.setShutdown(true);
      }
    }
    List<StramToNodeRequest> requests = new ArrayList<StramToNodeRequest>();
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
      ni.containerId = ns.containerId;
      ni.id = ns.node.id;
      ni.name = ns.node.getLogicalId();
      StreamingNodeHeartbeat hb = ns.lastHeartbeat;
      if (hb != null) {
        // initial heartbeat not yet received
        ni.status = hb.getState();
        ni.totalBytes = ns.bytesTotal;
        ni.totalTuples = ns.tuplesTotal;
        ni.lastHeartbeat = ns.lastHeartbeat.getGeneratedTms();
      }
      nodeInfoList.add(ni);
    }
    return nodeInfoList;
  }
  
}

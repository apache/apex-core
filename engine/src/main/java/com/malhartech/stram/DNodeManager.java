package com.malhartech.stram;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;

/**
 * Tracks topology provisioning/allocation to containers.
 */
public class DNodeManager {
  private static Logger LOG = LoggerFactory.getLogger(DNodeManager.class);
  
  private AtomicInteger nodeSequence = new AtomicInteger();
  private long windowStartMillis = System.currentTimeMillis();
  private long windowSizeMillis = 500;
  
  private class NodeStatus {
    private NodeStatus(StreamingNodeContext ctx) {
    }
    StreamingNodeHeartbeat lastHeartbeat;

    boolean isIdle() {
      return (lastHeartbeat != null && DNodeState.IDLE.name().equals(lastHeartbeat.getState()));
    }
  
  }

  /**
   * Nodes grouped for deployment, nodes connected with inline streams go to same container
   */
  private Map<NodeConf, Set<NodeConf>> nodeGroups = new HashMap<NodeConf, Set<NodeConf>>();
  
  
  private List<Set<NodeConf>> deployGroups = new ArrayList<Set<NodeConf>>();
  private Map<String, NodeStatus> allNodes = new ConcurrentHashMap<String, NodeStatus>();
  
  private Map<String, StreamingContainerContext> containerContextMap = new HashMap<String, StreamingContainerContext>();
  private Map<NodeConf, List<StreamingNodeContext>> logical2PhysicalMap = new ConcurrentHashMap<NodeConf, List<StreamingNodeContext>>();
  private Map<String, NodeConf> nodeId2NodeConfMap = new ConcurrentHashMap<String, NodeConf>();

  public DNodeManager(TopologyBuilder topology) {
      addNodes(topology.getAllNodes().values());
  }

  public int getNumRequiredContainers() {
    return deployGroups.size();
  }
  
  /**
   * Create node tracking context for logical node. Exposed here for tests.
   * @param dnodeId
   * @param nodeConf
   * @return
   */
  public static StreamingNodeContext createNodeContext(String dnodeId, NodeConf nodeConf) {
    StreamingNodeContext snc = new StreamingNodeContext();
    snc.setDnodeClassName(nodeConf.getProperties().get(TopologyBuilder.NODE_CLASSNAME));
    if (snc.getDnodeClassName() == null) {
      throw new IllegalArgumentException(String.format("Configuration for node '%s' is missing property '%s'", nodeConf.getId(), TopologyBuilder.NODE_CLASSNAME));
    }
    snc.setProperties(nodeConf.getProperties());
    snc.setLogicalId(nodeConf.getId());
    snc.setDnodeId(dnodeId);
    return snc;
  }

  /**
   * Group nodes and return the number of required containers
   */
  private int addNodes(Collection<NodeConf> nodes) {
    // group the nodes
    for (NodeConf nc : nodes) {
      // if the node has inline links to other nodes, cluster
      groupNodes(nc.getInputStreams());
      groupNodes(nc.getOutputStreams());
    }
    return nodeGroups.size();
  }

  private void groupNodes(Collection<StreamConf> streams) {
    for (StreamConf sc : streams) {
        if (sc.isInline()) {
          if (sc.getSourceNode() == null || sc.getTargetNode() == null) {
            LOG.error("Invalid inline setting on stream {}", sc);
          } else {
            groupNodes(sc.getSourceNode(), sc.getTargetNode());
          }
        } else {
          // single node grouping
          if (sc.getSourceNode() != null) {
            groupNodes(sc.getSourceNode());
          }
          if (sc.getTargetNode() != null) {
            groupNodes(sc.getTargetNode());
          }
        }
    }
  }

  private void groupNodes(NodeConf... nodes) {
    Set<NodeConf> group = null;
    for (NodeConf node : nodes) {
      group = nodeGroups.get(node);
      if (group != null) {
        break;
      }
    }
    if (group == null) {
      group = new HashSet<NodeConf>();
      this.deployGroups.add(group);
    }
    for (NodeConf node : nodes) {
      group.add(node);
      nodeGroups.put(node, group);
    }
  }
  
  /**
   * Find next group of nodes to deploy. There is no deployment dependency between groups of nodes
   * other than the requirement that buffer servers have to be deployed first. 
   * Inline stream dependencies are handled through the grouping.
   * Make best effort to deploy first groups w/o upstream dependencies else pick first group from list 
   */
  private Set<NodeConf> findDeployableNodeGroup() {
    // preference is to find a group that has no upstream dependencies
    // or they are already deployed
    for (Set<NodeConf> nodes : deployGroups) {
      boolean allInputsReady = true;
      for (NodeConf nodeConf : nodes) {
        if (nodeConf.getInputStreams().size() != 0) {
          // check if all inputs are deployed
          for (StreamConf streamConf : nodeConf.getInputStreams()) {
            NodeConf sourceNode = streamConf.getSourceNode();
            if (sourceNode != null && !streamConf.isInline()) {
              Set<NodeConf> sourceGroup = nodeGroups.get(sourceNode);
              if (nodes != sourceGroup && deployGroups.contains(sourceGroup)) {
                 LOG.debug("Skipping group {} as input dependency {} is not satisfied", nodes, sourceNode);
                  allInputsReady = false;
                  break;
              }
            }
          }
        }
      }
      if (allInputsReady) {
        return nodes;
      } else {
        break; // try next group
      }
    }
    return !deployGroups.isEmpty() ? deployGroups.get(0) : null;
  }

  /**
   * Find the stream context for the given stream, 
   * regardless of whether publisher or subscriber deploy first.
   * @param streamConf
   * @param nodeConf
   * @return
   */
  private StreamContext getStreamContext(StreamConf streamConf, InetSocketAddress bufferServerAddress) {
    for (StreamingContainerContext scc : this.containerContextMap.values()) {
        for (StreamContext sc : scc.getStreams()) {
            if (sc.getId().equals(streamConf.getId()) && sc.getTargetNodeLogicalId().equals(streamConf.getTargetNode().getId())) {
              return sc;
            }
        }
    }
    StreamContext sc = new StreamContext();
    sc.setId(streamConf.getId());
    sc.setBufferServerHost(bufferServerAddress.getHostName());
    sc.setBufferServerPort(bufferServerAddress.getPort());
    sc.setInline(streamConf.isInline());

    // map logical node id to assigned sequences for source and target
    if (streamConf.getSourceNode() != null) {
      sc.setSourceNodeId(getNodeContext(streamConf.getSourceNode()).getDnodeId());
    } else {
      // input adapter, need implementation class
      throw new UnsupportedOperationException("input adapter not implemented");
    }
    if (streamConf.getTargetNode() != null) {
      sc.setTargetNodeId(getNodeContext(streamConf.getTargetNode()).getDnodeId());
      sc.setTargetNodeLogicalId(streamConf.getTargetNode().getId());
    } else {
      // external output, need implementation class
      throw new UnsupportedOperationException("only output to node implemented");
    }
    return sc;
  }
  
  /**
   * Assign streaming nodes to newly available container. Multiple nodes can run in a container.  
   * @param containerId
   * @param bufferServerAddress Buffer server for publishers on the container.
   * @return
   */
  public synchronized StreamingContainerContext assignContainer(String containerId, InetSocketAddress bufferServerAddress) {
    if (deployGroups.isEmpty()) {
      throw new IllegalStateException("There are no nodes to deploy.");
    }
    Set<NodeConf> nodes = findDeployableNodeGroup();
    if (nodes == null) {
      throw new IllegalStateException("Cannot find a streaming node for new container, remaining unassgined nodes are " + this.deployGroups);
    }
    deployGroups.remove(nodes);
   
    List<StreamingNodeContext> nodeContextList = new ArrayList<StreamingNodeContext>(nodes.size());
    for (NodeConf nodeConf : nodes) {
      nodeContextList.add(getNodeContext(nodeConf));
    }
    
    StreamingContainerContext scc = new StreamingContainerContext();
    scc.setWindowSizeMillis(this.windowSizeMillis);
    scc.setStartWindowMillis(this.windowStartMillis);
    scc.setNodes(nodeContextList);

    // assemble connecting streams for deployment group of node(s)
    // map to remove duplicates between nodes in same container (inline or not)
    Map<String, StreamContext> streams = new HashMap<String, StreamContext>();
    for (StreamingNodeContext snc  : scc.getNodes()) {
      NodeConf nodeConf = nodeId2NodeConfMap.get(snc.getDnodeId());
      // DAG node inputs
      for (StreamConf streamConf : nodeConf.getInputStreams()) {
        streams.put(streamConf.getId(), getStreamContext(streamConf, bufferServerAddress));
      }
      // DAG node outputs
      for (StreamConf streamConf : nodeConf.getOutputStreams()) {
        streams.put(streamConf.getId(), getStreamContext(streamConf, bufferServerAddress));
      }
    }
    scc.setStreams(new ArrayList<StreamContext>(streams.values()));
    containerContextMap.put(containerId, scc);

    return scc;
  }
 
  private StreamingNodeContext getNodeContext(NodeConf nodeConf) {
    synchronized (logical2PhysicalMap) {
      if (logical2PhysicalMap.containsKey(nodeConf)) {
          return logical2PhysicalMap.get(nodeConf).get(0);
      }
      StreamingNodeContext scc = createNodeContext(""+nodeSequence.incrementAndGet(), nodeConf);
      logical2PhysicalMap.put(nodeConf, Collections.singletonList(scc));
      nodeId2NodeConfMap.put(scc.getDnodeId(), nodeConf);
      allNodes.put(scc.getDnodeId(), new NodeStatus(scc));
      return scc;
    }
  }
  
  public StreamingContainerContext getContainerContext(String containerId) {
    StreamingContainerContext ctx = containerContextMap.get(containerId);
    if (ctx == null) {
      throw new IllegalArgumentException("No context for container " + containerId);
    }
    return ctx;
  }

  public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat heartbeat) {
    boolean containerIdle = true;
    
    for (StreamingNodeHeartbeat shb : heartbeat.getDnodeEntries()) {
      ReflectionToStringBuilder b = new ReflectionToStringBuilder(shb);
      LOG.info("node {} heartbeat: {}", shb.getNodeId(), b.toString());

      NodeStatus nodeStatus = allNodes.get(shb.getNodeId());
      if (nodeStatus == null) {
         LOG.error("Heartbeat for unknown node {} (container {})", shb.getNodeId(), heartbeat.getContainerId());
         continue;
      }
      nodeStatus.lastHeartbeat = shb;
      if (!nodeStatus.isIdle()) {
        containerIdle = false;
        checkNodeLoad(shb);
      }
    }
    
    List<StramToNodeRequest> requests = new ArrayList<StramToNodeRequest>(); 
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    if (containerIdle && isApplicationIdle()) {
      LOG.info("requesting shutdown for container {}", heartbeat.getContainerId());
      rsp.setShutdown(true);
    }
    rsp.setNodeRequests(requests);
    return rsp;
  }

  private boolean isApplicationIdle() {
    for (NodeStatus nodeStatus : this.allNodes.values()) {
      if (!nodeStatus.isIdle()) {
        return false;
      }
    }
    return true;
  }
  
  private void checkNodeLoad(StreamingNodeHeartbeat shb) {
      NodeConf nodeConf = nodeId2NodeConfMap.get(shb.getNodeId());
      // TODO: synchronization
      if (nodeConf == null) {
          LOG.warn("Cannot find the configuration for node {}", shb.getNodeId());
          return;
      }
      // check load constraints
      int tuplesProcessed = shb.getNumberTuplesProcessed();
      // TODO: populate into bean at initialization time
      Map<String, String> properties = nodeConf.getProperties();
      if (properties.containsKey(TopologyBuilder.NODE_LB_TUPLECOUNT_MIN)) {
         int minTuples = new Integer(properties.get(TopologyBuilder.NODE_LB_TUPLECOUNT_MIN));
         if (tuplesProcessed < minTuples) {
           LOG.warn("Node {} processed {} messages below configured min {}", new Object[] { shb.getNodeId(), tuplesProcessed, minTuples });
         }
      }
      if (properties.containsKey(TopologyBuilder.NODE_LB_TUPLECOUNT_MAX)) {
        int maxTuples = new Integer(properties.get(TopologyBuilder.NODE_LB_TUPLECOUNT_MAX));
        if (tuplesProcessed > maxTuples) {
           LOG.warn("Node {} processed {} messages and exceeds configured max {}", new Object[] { shb.getNodeId(), tuplesProcessed, maxTuples });
        }
     }
      
  }
  
  
}

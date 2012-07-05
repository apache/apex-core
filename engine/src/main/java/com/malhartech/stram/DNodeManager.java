package com.malhartech.stram;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private class NodeStatus {
    private NodeStatus(StreamingNodeContext ctx) {
      this.context = ctx;
    }
    final StreamingNodeContext context;
    StreamingNodeHeartbeat lastHeartbeat;

    boolean isIdle() {
      return (lastHeartbeat != null && DNodeState.IDLE.name().equals(lastHeartbeat.getState()));
    }
  
  }
  
  private List<NodeConf> unassignedNodes = new ArrayList<NodeConf>();
  private List<NodeConf> runningNodes = new ArrayList<NodeConf>();
  private Map<String, NodeStatus> allNodes = new ConcurrentHashMap<String, NodeStatus>();
  
  private Map<String, StreamingContainerContext> containerContextMap = new HashMap<String, StreamingContainerContext>();
  private Map<NodeConf, List<StreamingNodeContext>> logical2PhysicalMap = new ConcurrentHashMap<NodeConf, List<StreamingNodeContext>>();
  private Map<String, NodeConf> nodeId2NodeConfMap = new ConcurrentHashMap<String, NodeConf>();
  
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

  public void addNodes(Collection<NodeConf> nodes) {
    this.unassignedNodes.addAll(nodes);
  }
  
  /**
   * Find unassigned streaming node. This can be a start node or a node that has
   * all inputs satisfied (node can only be initialized with upstream buffer servers known).
   */
  private NodeConf findDeployableNode() {
    
    for (NodeConf nodeConf : unassignedNodes) {
      if (nodeConf.getInputStreams().size() == 0) {
        // no input dependency
        return nodeConf;
      } else {
        boolean allInputsReady = true;
        // check if all inputs are allocated
        for (StreamConf streamConf : nodeConf.getInputStreams()) {
            NodeConf sourceNode = streamConf.getSourceNode();
            if (sourceNode != null && unassignedNodes.contains(sourceNode)) {
                LOG.debug("Cannot allocate node {} because input dependency {} is not satisfied", nodeConf, sourceNode);
                allInputsReady = false;
                break;
            }
        }
        if (allInputsReady) {
          return nodeConf;
        }
      }
    }
    return null;
  }

  /**
   * Find the publisher for the given stream.
   * Upstream nodes must be deployed first.
   * @param streamConf
   * @param nodeConf
   * @return
   */
  private StreamContext findInputStreamContext(StreamConf streamConf) {
    for (StreamingContainerContext scc : this.containerContextMap.values()) {
        for (StreamContext sc : scc.getStreams()) {
            if (sc.getId().equals(streamConf.getId()) && sc.getTargetNodeLogicalId().equals(streamConf.getTargetNode().getId())) {
              return sc;
            }
        }
    }
    return null;
  }
  
  /**
   * Assign streaming nodes to newly available container. Multiple nodes can run in a container.  
   * @param containerId
   * @param defaultbufferServerAddress Buffer server for publishers on the container.
   * @return
   */
  public synchronized StreamingContainerContext assignContainer(String containerId, InetSocketAddress defaultbufferServerAddress) {
    if (unassignedNodes.isEmpty()) {
      throw new IllegalStateException("There are no nodes waiting for launch.");
    }
    NodeConf nodeConf = findDeployableNode();
    if (nodeConf == null) {
      throw new IllegalStateException("Cannot find a streaming node for new container, remaining unassgined nodes are " + this.unassignedNodes);
    }
    unassignedNodes.remove(nodeConf);
   
    StreamingNodeContext snc = getNodeContext(nodeConf);

    StreamingContainerContext scc = new StreamingContainerContext();
    scc.setNodes(Collections.singletonList(snc));

    // assemble connecting streams for the node
    List<StreamContext> streams = new ArrayList<StreamContext>(nodeConf.getOutputStreams().size());

    // DAG node inputs
    for (StreamConf input : nodeConf.getInputStreams()) {
      StreamContext sc = new StreamContext();
      sc.setId(input.getId());
      sc.setInline(false); // TODO
      
      // get buffer server for input
      StreamContext inputStreamContext = findInputStreamContext(input);
      if (inputStreamContext == null) {
        // should not happen as upstream nodes are deployed first
        throw new IllegalStateException("Cannot find the input container for stream " + input.getId());
      }
      sc.setBufferServerHost(inputStreamContext.getBufferServerHost());
      sc.setBufferServerPort(inputStreamContext.getBufferServerPort());

      // map the logical node id to assigned sequences for source and target
      sc.setTargetNodeId(snc.getDnodeId());
      if (input.getSourceNode() != null) {
        sc.setTargetNodeId(getNodeContext(input.getSourceNode()).getDnodeId());
        sc.setTargetNodeLogicalId(input.getSourceNode().getId());
      } else {
        throw new UnsupportedOperationException("only input from node implemented");
      }
      streams.add(sc);
    }
    
    // DAG node outputs
    for (StreamConf output : nodeConf.getOutputStreams()) {
        StreamContext sc = new StreamContext();
        sc.setId(output.getId());
        sc.setInline(false); // TODO
        sc.setBufferServerHost(defaultbufferServerAddress.getHostName());
        sc.setBufferServerPort(defaultbufferServerAddress.getPort());
        // map the logical node id to assigned sequences for source and target
        sc.setSourceNodeId(snc.getDnodeId());
        if (output.getTargetNode() != null) {
          sc.setTargetNodeId(getNodeContext(output.getTargetNode()).getDnodeId());
          sc.setTargetNodeLogicalId(output.getTargetNode().getId());
        } else {
          throw new UnsupportedOperationException("only output to node implemented");
        }
        streams.add(sc);
    }
        
    scc.setStreams(streams);
    containerContextMap.put(containerId, scc);
    runningNodes.add(nodeConf);
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

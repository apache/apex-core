package com.malhartech.stram;

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

import com.malhartech.dag.DNode.DNodeState;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeat;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat;
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
   * Assign streaming nodes to newly available container. Multiple nodes can run in a container.  
   * @param containerId
   * @return
   */
  public StreamingContainerContext assignContainer(String containerId) {
    if (unassignedNodes.isEmpty()) {
      throw new IllegalStateException("There are no nodes waiting for launch.");
    }
    
    // TODO: policy for assigning node(s) to containers
    NodeConf nodeConf = unassignedNodes.remove(0);
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
        // map the logical node id to assigned sequences for source and target
        sc.setSourceNodeId(snc.getDnodeId());
        if (output.getTargetNode() != null) {
          sc.setTargetNodeId(getNodeContext(output.getTargetNode()).getDnodeId());
          sc.setTargetNodeLogicalId(output.getTargetNode().getId());
        } else {
          throw new UnsupportedOperationException("only input from node implemented");
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

package com.malhartech.stram;

import java.util.ArrayList;
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
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingNodeHeartbeat;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;

/**
 * Tracks topology provisioning to containers.
 */
public class DNodeManager {
  private static Logger LOG = LoggerFactory.getLogger(DNodeManager.class);
  
  private AtomicInteger nodeSequence = new AtomicInteger();
  
  public List<NodeConf> unassignedNodes = new ArrayList<NodeConf>();
  public List<NodeConf> runningNodes = new ArrayList<NodeConf>();

  private Map<String, StreamingContainerContext> containerContextMap = new HashMap<String, StreamingContainerContext>();
  private Map<NodeConf, List<StreamingNodeContext>> logical2PhysicalMap = new ConcurrentHashMap<NodeConf, List<StreamingNodeContext>>();

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
    String idleNodeId = null;
    
    for (StreamingNodeHeartbeat shb : heartbeat.getDnodeEntries()) {
      ReflectionToStringBuilder b = new ReflectionToStringBuilder(shb);
      LOG.info("node {} heartbeat: {}", shb.getNodeId(), b.toString());

      if (!DNodeState.IDLE.name().equals(shb.getState())) {
        // container is active if at least one streaming node is active
        // TODO; this should be: container is active if at least on input node is active
        containerIdle = false;
      } else {
        // find the node
        idleNodeId = shb.getNodeId();
      }
    }
    
    List<StramToNodeRequest> requests = new ArrayList<StramToNodeRequest>(); 
    if (containerIdle == true) {
      // TODO: should be at container, not node level
      LOG.info("sending shutdown request for nodes in container {}", heartbeat.getContainerId());
      StramToNodeRequest req = new StramToNodeRequest();
      req.setNodeId(idleNodeId);
      req.setRequestType(RequestType.SHUTDOWN);
      requests.add(req);
    }
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.setNodeRequests(requests);
    return rsp;
  }
  
}

package com.malhar.app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.malhar.app.StreamingNodeUmbilicalProtocol.ContainerHeartbeat;
import com.malhar.app.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhar.app.StreamingNodeUmbilicalProtocol.StreamContext;
import com.malhar.app.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhar.app.StreamingNodeUmbilicalProtocol.StreamingNodeContext;
import com.malhar.stram.conf.TopologyBuilder;
import com.malhar.stram.conf.TopologyBuilder.NodeConf;
import com.malhar.stram.conf.TopologyBuilder.StreamConf;

public class DNodeManager {

  private AtomicInteger nodeSequence = new AtomicInteger();
  
  public List<NodeConf> unassignedNodes = new ArrayList<NodeConf>();
  public List<NodeConf> runningNodes = new ArrayList<NodeConf>();

  private Map<String, StreamingContainerContext> containerContextMap = new HashMap<String, StreamingContainerContext>();
  private Map<NodeConf, List<StreamingNodeContext>> logical2PhysicalMap = new ConcurrentHashMap<NodeConf, List<StreamingNodeContext>>();
  
  public StreamingContainerContext assignContainer(String containerId) {
    if (unassignedNodes.isEmpty()) {
      throw new IllegalStateException("There are no nodes waiting for launch.");
    }
    // TODO: pick nodes as per topology
    NodeConf nodeConf = unassignedNodes.remove(0);
    StreamingNodeContext snc = getNodeContext(nodeConf);

    StreamingContainerContext scc = new StreamingContainerContext();
    scc.setNodes(Collections.singletonList(snc));

    // assemble connecting streams for the node
    List<StreamContext> streams = new ArrayList<StreamContext>(nodeConf.getOutputStreams().size());

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
    
    // DAG node inputs
    for (StreamConf input : nodeConf.getInputStreams()) {
      StreamContext sc = new StreamContext();
      sc.setId(input.getId());
      sc.setInline(false); // TODO
      // map the logical node id to assigned sequences for source and target
      sc.setTargetNodeId(snc.getDnodeId());
      if (input.getSourceNode() != null) {
        sc.setTargetNodeId(getNodeContext(nodeConf).getDnodeId());
        sc.setTargetNodeLogicalId(nodeConf.getId());
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

  public StreamingContainerContext getContainerContext(String containerId) {
    StreamingContainerContext ctx = containerContextMap.get(containerId);
    if (ctx == null) {
      throw new IllegalArgumentException("No context for container " + containerId);
    }
    return ctx;
  }

  public ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat heartbeat) {
    return null;
  }
  
}

package com.malhar.app;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.yarn.api.records.ContainerId;

import com.malhar.app.StreamingNodeUmbilicalProtocol.StreamingNodeContext;
import com.malhar.stram.conf.TopologyBuilder;
import com.malhar.stram.conf.TopologyBuilder.NodeConf;

public class DNodeManager {

  private AtomicInteger nodeSequence = new AtomicInteger();
  
  public static class NodeState {
    private NodeConf nodeConf;
    private ContainerId assignedContainer;
    private String lastWindow;
    private long lastWindowCompleteTms;
    private int tuplesProcessed;
  }

  public List<NodeConf> unassignedNodes = new ArrayList<NodeConf>();
  public List<NodeConf> runningNodes = new ArrayList<NodeConf>();
  public List<NodeConf> failedNodes;

  private Map<String, StreamingNodeContext> containerContextMap = new HashMap<String, StreamingNodeContext>();

  public StreamingNodeContext assignNodesToContainer(String containerId) {
    if (unassignedNodes.isEmpty()) {
      throw new IllegalStateException("There are no nodes waiting for launch.");
    }
    // TODO: simply picking first unassigned node may not be what we want?
    NodeConf nodeConf = unassignedNodes.remove(0);
    StreamingNodeContext sc = createNodeContext(""+nodeSequence.incrementAndGet(), nodeConf, containerId);
    containerContextMap.put(containerId, sc);
    runningNodes.add(nodeConf);
    return sc;
  }
  
  public static StreamingNodeContext createNodeContext(String dnodeId, NodeConf nodeConf, String containerId) {
    StreamingNodeContext snc = new StreamingNodeContext();
    snc.dnodeClassName = nodeConf.getProperties().get(TopologyBuilder.NODE_CLASSNAME);
    if (snc.dnodeClassName == null) {
      throw new IllegalArgumentException(String.format("Configuration for node '%s' is missing property '%s'", nodeConf.getId(), TopologyBuilder.NODE_CLASSNAME));
    }
    snc.properties = nodeConf.getProperties();
    snc.logicalId = nodeConf.getId();
    snc.dnodeId = dnodeId;
    return snc;
  }

  public StreamingNodeContext getContext(String containerId) {
    StreamingNodeContext ctx = containerContextMap.get(containerId);
    if (ctx == null) {
      throw new IllegalArgumentException("No context for container " + containerId);
    }
    return ctx;
  }
  
}

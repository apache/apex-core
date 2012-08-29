/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.TopologyDeployer.PTContainer;
import com.malhartech.stram.TopologyDeployer.PTNode;

/**
 * 
 * Representation of a child container in the master<p>
 * <br>
 */
public class StramChildAgent {
  private static Logger LOG = LoggerFactory.getLogger(StramChildAgent.class);

  public static class DeployRequest {
    final AtomicInteger ackCountdown;
    final PTContainer container;
    final AtomicInteger executeWhenZero; 
    private List<NodeDeployInfo> nodes;
    Map<PTNode, Long> checkpoints;
    
    public DeployRequest(PTContainer container, AtomicInteger ackCountdown) {
      this.container = container;
      this.ackCountdown = ackCountdown;
      this.executeWhenZero = null;
    }
    public DeployRequest(PTContainer container, AtomicInteger ackCountdown, AtomicInteger executeWhenZero) {
      this.container = container;
      this.ackCountdown = ackCountdown;
      this.executeWhenZero = executeWhenZero;
    }
    
    void cancel() {
      if (ackCountdown != null) {
        ackCountdown.set(-1);
      }
    }
    
    void ack() {
      ackCountdown.decrementAndGet();
    }
    
    void setNodes(List<NodeDeployInfo> nodes) {
      this.nodes = nodes;
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("nodes", this.nodes)
        //.append("streams", this.streams)
        .append("executeWhenZero", this.executeWhenZero)
        .toString();
    }
  }

  public static class UndeployRequest extends DeployRequest {
    public UndeployRequest(PTContainer container,
        AtomicInteger ackCountdown, AtomicInteger executeWhenZero) {
      super(container, ackCountdown, executeWhenZero);
    }
  }
  
  public StramChildAgent(PTContainer container, StreamingContainerContext initCtx) {
    this.container = container;
    this.initCtx = initCtx;
  }
  
  boolean shutdownRequested = false;
  boolean isComplete = false;
 // StreamingContainerContext containerContext;
  long lastHeartbeatMillis = 0;
  long lastCheckpointRequestMillis = 0;
  long createdMillis = System.currentTimeMillis();
  final PTContainer container;
  final StreamingContainerContext initCtx;
  DeployRequest pendingRequest = null;
  
  private ConcurrentLinkedQueue<DeployRequest> requests = new ConcurrentLinkedQueue<DeployRequest>();

  public StreamingContainerContext getInitContext() {
    ContainerHeartbeatResponse rsp = pollRequest();
    if (rsp != null && rsp.getDeployRequest() != null) {
      initCtx.nodeList = rsp.getDeployRequest().nodeList;
    }
    return initCtx;
  }

  public boolean hasPendingWork() {
    return !this.requests.isEmpty() || this.pendingRequest != null;
  }
  
  private void ackPendingRequest() {
    if (pendingRequest != null) {
      if (pendingRequest.ackCountdown != null) {
        pendingRequest.ackCountdown.decrementAndGet();
        pendingRequest = null;
      }
    }
  }
  
  public void addRequest(DeployRequest r) {
    this.requests.add(r);
    LOG.info("Adding request {} {}", container.containerId, r);
  }
  
  public ContainerHeartbeatResponse pollRequest() {
    ackPendingRequest();
    //LOG.debug("Number of pending requests for container {}: {}", this.container.containerId, requests.size());    
    DeployRequest r = requests.peek();
    if (r == null) {
      return null;
    }

    if (r.executeWhenZero != null) {
      if (r.executeWhenZero.get() < 0) {
        // cancelled
        return null;
      } else if (r.executeWhenZero.get() > 0) {
        ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
        LOG.debug("Request for {} blocked: {}", this.container.containerId, r);
        rsp.setPendingRequests(true);
        // keep polling
        return rsp;
      }
    }

    // process
    if (!requests.remove(r)) {
        return null;
    }
    
    this.pendingRequest = r;
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    if (r.nodes != null) {
      StreamingContainerContext scc = new StreamingContainerContext();
      scc.nodeList = r.nodes;
      if (r instanceof UndeployRequest) {
        rsp.setUndeployRequest(scc);
      } else {
        rsp.setDeployRequest(scc);
      }
    }
    
    rsp.setPendingRequests(!this.requests.isEmpty());
    return rsp;
  }
}

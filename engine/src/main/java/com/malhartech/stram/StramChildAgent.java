/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.PhysicalPlan.PTContainer;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;

/**
 *
 * Representation of a child container in the master<p>
 * <br>
 */
public class StramChildAgent {
  private static final Logger LOG = LoggerFactory.getLogger(StramChildAgent.class);

  public static class DeployRequest {
    final AtomicInteger ackCountdown;
    final PTContainer container;
    final AtomicInteger executeWhenZero;
    private List<OperatorDeployInfo> nodes;
    Map<PTOperator, Long> checkpoints;

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

    void setNodes(List<OperatorDeployInfo> nodes) {
      this.nodes = nodes;
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("operators", this.nodes)
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

  class MovingAverage {
    private final int periods;
    private final long[] values;
    private int index = 0;
    private boolean filled = false;

    MovingAverage(int periods) {
      this.periods = periods;
      this.values = new long[periods];
    }

    void add(long val) {
      values[index++] = val;
      if (index == periods) {
        filled = true;
      }
      index = index % periods;
    }

    long getAvg() {
      long sum = 0;
      for (int i=0; i<periods; i++) {
        sum += values[i];
      }

      if (!filled) {
        return index == 0 ? 0 : sum/index;
      } else {
        return sum/periods;
      }
    }
  }

  class OperatorStatus
  {
    StreamingNodeHeartbeat lastHeartbeat;
    final PTOperator operator;
    final PTContainer container;
    int totalTuplesProcessed;
    int totalTuplesEmitted;
    MovingAverage tuplesProcessedPSMA10 = new MovingAverage(10);
    MovingAverage tuplesEmittedPSMA10 = new MovingAverage(10);

    private OperatorStatus(PTContainer container, PTOperator operator) {
      this.operator = operator;
      this.container = container;
    }

    public boolean isIdle()
    {
      if ((lastHeartbeat != null && DNodeState.IDLE.name().equals(lastHeartbeat.getState()))) {
        return true;
      }
      return false;
    }
  }

  public StramChildAgent(PTContainer container, StreamingContainerContext initCtx) {
    this.container = container;
    this.initCtx = initCtx;
    this.operators = new HashMap<String, OperatorStatus>(container.operators.size());
    for (PTOperator operator : container.operators) {
      this.operators.put(operator.id, new OperatorStatus(container, operator));
    }
  }

  boolean shutdownRequested = false;
  boolean isComplete = false;
  long lastHeartbeatMillis = 0;
  long lastCheckpointRequestMillis = 0;
  long createdMillis = System.currentTimeMillis();
  final PTContainer container;
  final Map<String, OperatorStatus> operators;
  final StreamingContainerContext initCtx;
  DeployRequest pendingRequest = null;

  private final ConcurrentLinkedQueue<DeployRequest> requests = new ConcurrentLinkedQueue<DeployRequest>();

  public StreamingContainerContext getInitContext() {
    ContainerHeartbeatResponse rsp = pollRequest();
    if (rsp != null && rsp.deployRequest != null) {
      initCtx.nodeList = rsp.deployRequest;
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

  protected ConcurrentLinkedQueue<DeployRequest> getRequests() {
    return this.requests;
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
        rsp.hasPendingRequests = true;
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
        rsp.undeployRequest = scc.nodeList;
      } else {
        rsp.deployRequest = scc.nodeList;
      }
    }

    rsp.hasPendingRequests = (!this.requests.isEmpty());
    return rsp;
  }

  boolean isIdle() {
    for (OperatorStatus operatorStatus : this.operators.values()) {
      if (!operatorStatus.isIdle()) {
        return false;
      }
    }
    return true;
  }

}

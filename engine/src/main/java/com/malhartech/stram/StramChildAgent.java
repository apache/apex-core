/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.DAG.InputPortMeta;
import com.malhartech.api.DAG.StreamMeta;
import com.malhartech.api.InputOperator;
import com.malhartech.api.Operator;
import com.malhartech.api.OperatorCodec;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.stram.OperatorDeployInfo.InputDeployInfo;
import com.malhartech.stram.OperatorDeployInfo.OutputDeployInfo;
import com.malhartech.stram.PhysicalPlan.PTContainer;
import com.malhartech.stram.PhysicalPlan.PTInput;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.PhysicalPlan.PTOutput;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;
import com.malhartech.stram.webapp.ContainerInfo;

/**
 *
 * Representation of a child container in the master<p>
 * <br>
 */
public class StramChildAgent {
  private static final Logger LOG = LoggerFactory.getLogger(StramChildAgent.class);

  public static class ContainerStartRequest extends DeployRequest {
    final PTContainer container;

    ContainerStartRequest(PTContainer container, AtomicInteger ackCountdown, AtomicInteger executeWhenZero) {
      super(ackCountdown, executeWhenZero);
      this.container = container;
    }
  }

  public static class DeployRequest {
    final AtomicInteger ackCountdown;
    final AtomicInteger executeWhenZero;
    private List<PTOperator> deployOperators;

    public DeployRequest(AtomicInteger ackCountdown, AtomicInteger executeWhenZero) {
      this.ackCountdown = ackCountdown;
      this.executeWhenZero = executeWhenZero;
    }

    void cancel() {
      if (ackCountdown != null) {
        //LOG.debug("cancelling: " + this);
        ackCountdown.set(-1);
      }
    }

    void ack() {
      ackCountdown.decrementAndGet();
    }

    void setOperators(List<PTOperator> operators) {
      this.deployOperators = operators;
    }

    List<PTOperator> getOperators() {
      return this.deployOperators;
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("operators", this.deployOperators)
        //.append("streams", this.streams)
        .append("executeWhenZero", this.executeWhenZero)
        .toString();
    }
  }

  public static class UndeployRequest extends DeployRequest {
    public UndeployRequest(PTContainer container,
        AtomicInteger ackCountdown, AtomicInteger executeWhenZero) {
      super(ackCountdown, executeWhenZero);
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
    long totalTuplesProcessed;
    long totalTuplesEmitted;
    long currentWindowId;
    MovingAverage tuplesProcessedPSMA10 = new MovingAverage(10);
    MovingAverage tuplesEmittedPSMA10 = new MovingAverage(10);
    String recordingName; // null if recording is not in progress

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
    this.operators = new HashMap<Integer, OperatorStatus>(container.operators.size());
  }

  boolean shutdownRequested = false;
  boolean isComplete = false;
  long lastHeartbeatMillis = 0;
  long lastCheckpointRequestMillis = 0;
  long createdMillis = System.currentTimeMillis();
  final PTContainer container;
  Map<Integer, OperatorStatus> operators;
  final StreamingContainerContext initCtx;
  private final OperatorCodec nodeSerDe = StramUtils.getNodeSerDe(null);
  DeployRequest pendingRequest = null;

  private final ConcurrentLinkedQueue<DeployRequest> requests = new ConcurrentLinkedQueue<DeployRequest>();
  private final ConcurrentLinkedQueue<StramToNodeRequest> operatorRequests = new ConcurrentLinkedQueue<StramToNodeRequest>();

  public StreamingContainerContext getInitContext() {
    return initCtx;
  }

  public boolean hasPendingWork() {
    return !this.requests.isEmpty() || this.pendingRequest != null;
  }

  private void ackPendingRequest() {
    if (pendingRequest != null) {
      if (pendingRequest.ackCountdown != null) {
        pendingRequest.ackCountdown.decrementAndGet();
        //LOG.debug("ack {} {}", pendingRequest.ackCountdown, pendingRequest);
        pendingRequest = null;
      }
    }
  }

  public void addRequest(DeployRequest r) {
    this.requests.add(r);
    if (r.deployOperators != null) {
      // currently deployed operators copy on write
      HashMap<Integer, OperatorStatus> newDeployedOperators = new HashMap<Integer, OperatorStatus>(this.operators);
      if (r instanceof UndeployRequest) {
        for (PTOperator operator : r.deployOperators) {
            newDeployedOperators.remove(operator.getId());
        }
      } else {
        for (PTOperator operator : r.deployOperators) {
          newDeployedOperators.put(operator.getId(), new OperatorStatus(container, operator));
        }
      }
      this.operators = Collections.unmodifiableMap(newDeployedOperators);
    }
    LOG.info("Added request {} {}"/*ack=" + r.ackCountdown + " ewz=" + r.executeWhenZero*/, container.containerId, r);
  }

  public void addOperatorRequest(StramToNodeRequest r) {
    this.operatorRequests.add(r);
    LOG.info("Adding operator request {} {}", container.containerId, r);
  }

  protected ConcurrentLinkedQueue<DeployRequest> getRequests() {
    return this.requests;
  }

  protected ConcurrentLinkedQueue<StramToNodeRequest> getOperatorRequests() {
    return this.operatorRequests;
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
        //LOG.debug("cancelled " + this);
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
    if (r.deployOperators != null) {
      if (r instanceof UndeployRequest) {
        List<OperatorDeployInfo> nodeList = getUndeployInfoList(r.deployOperators);
        rsp.undeployRequest = nodeList;
      } else {
        List<OperatorDeployInfo> nodeList = getDeployInfoList(r.deployOperators);
        rsp.deployRequest = nodeList;
      }
    }

    rsp.hasPendingRequests = (!this.requests.isEmpty());
    return rsp;
  }

  boolean isIdle() {
    if (this.hasPendingWork()) {
      // container may have no active operators but deploy request pending
      return false;
    }
    for (OperatorStatus operatorStatus : this.operators.values()) {
      if (!operatorStatus.isIdle()) {
        return false;
      }
    }
    return true;
  }

  public List<OperatorDeployInfo> getDeployInfo() {
    return getDeployInfoList(container.operators);
  }

  /**
   * Create deploy info for StramChild.
   * @param operators
   * @return StreamingContainerContext
   */
  private List<OperatorDeployInfo> getDeployInfoList(List<PTOperator> operators) {

    if (container.bufferServerAddress == null) {
      throw new IllegalStateException("No buffer server address assigned");
    }

    Map<OperatorDeployInfo, PTOperator> nodes = new LinkedHashMap<OperatorDeployInfo, PTOperator>();
    Map<String, OutputDeployInfo> publishers = new LinkedHashMap<String, OutputDeployInfo>();

    for (PTOperator node : operators) {
      OperatorDeployInfo ndi = createOperatorDeployInfo(node);
      long checkpointWindowId = node.getRecoveryCheckpoint();
      if (checkpointWindowId > 0) {
        LOG.debug("Operator {} recovery checkpoint {}", node.getId(), Codec.getStringWindowId(checkpointWindowId));
        ndi.checkpointWindowId = checkpointWindowId;
      }
      nodes.put(ndi, node);
      ndi.inputs = new ArrayList<InputDeployInfo>(node.inputs.size());
      ndi.outputs = new ArrayList<OutputDeployInfo>(node.outputs.size());

      for (PTOutput out : node.outputs) {
        final StreamMeta streamMeta = out.logicalStream;
        // buffer server or inline publisher
        OutputDeployInfo portInfo = new OutputDeployInfo();
        portInfo.declaredStreamId = streamMeta.getId();
        portInfo.portName = out.portName;
        portInfo.contextAttributes = streamMeta.getSource().getAttributes();

        if (!out.isDownStreamInline()) {
          portInfo.bufferServerHost = node.container.bufferServerAddress.getHostName();
          portInfo.bufferServerPort = node.container.bufferServerAddress.getPort();
          if (streamMeta.getCodecClass() != null) {
            portInfo.serDeClassName = streamMeta.getCodecClass().getName();
          }
        } else {
          //LOG.debug("Inline stream {}", out);
          // target set below
          //portInfo.inlineTargetNodeId = "-1subscriberInOtherContainer";
        }

        ndi.outputs.add(portInfo);
        publishers.put(node.getId() + "/" + streamMeta.getId(), portInfo);
      }
    }

    // after we know all publishers within container, determine subscribers

    for (Map.Entry<OperatorDeployInfo, PTOperator> nodeEntry : nodes.entrySet()) {
      OperatorDeployInfo ndi = nodeEntry.getKey();
      PTOperator node = nodeEntry.getValue();
      for (PTInput in : node.inputs) {
        final StreamMeta streamMeta = in.logicalStream;
        // input from other node(s) OR input adapter
        if (streamMeta.getSource() == null) {
          throw new AssertionError("source is null: " + in);
        }
        PTOutput sourceOutput = in.source;

        InputDeployInfo inputInfo = new InputDeployInfo();
        inputInfo.declaredStreamId = streamMeta.getId();
        inputInfo.portName = in.portName;
        for (Map.Entry<InputPortMeta, StreamMeta> e : node.getOperatorMeta().getInputStreams().entrySet()) {
          if (e.getValue() == streamMeta) {
            inputInfo.contextAttributes = e.getKey().getAttributes();
          }
        }
        inputInfo.sourceNodeId = sourceOutput.source.getId();
        inputInfo.sourcePortName = sourceOutput.portName;
        if (in.partitions != null) {
          inputInfo.partitionKeys = in.partitions.partitions;
          inputInfo.partitionMask = in.partitions.mask;
        }

        if (streamMeta.isInline() && sourceOutput.source.container == node.container) {
          // inline input (both operators in same container and inline hint set)
          OutputDeployInfo outputInfo = publishers.get(sourceOutput.source.getId() + "/" + streamMeta.getId());
          if (outputInfo == null) {
            throw new AssertionError("Missing publisher for inline stream " + sourceOutput);
          }
        } else {
          // buffer server input
          // FIXME: address to come from upstream output port, should be assigned first
          InetSocketAddress addr = sourceOutput.source.container.bufferServerAddress;
          if (addr == null) {
            // TODO: occurs during undeploy
            LOG.warn("upstream address not assigned: " + sourceOutput);
            addr = container.bufferServerAddress;
            //throw new IllegalStateException("upstream address not assigned: " + in.source);
          }
          inputInfo.bufferServerHost = addr.getHostName();
          inputInfo.bufferServerPort = addr.getPort();
          if (streamMeta.getCodecClass() != null) {
            inputInfo.serDeClassName = streamMeta.getCodecClass().getName();
          }
        }
        ndi.inputs.add(inputInfo);
      }
    }

    return new ArrayList<OperatorDeployInfo>(nodes.keySet());
  }

  /**
   * Create operator undeploy request for StramChild. Since the physical plan
   * could have been modified (dynamic partitioning etc.), stream information
   * cannot be provided. StramChild keeps track of connected streams and removes
   * them along with the operator instance.
   *
   * @param operators
   * @return
   */
  private List<OperatorDeployInfo> getUndeployInfoList(List<PTOperator> operators) {
    List<OperatorDeployInfo> undeployList = new ArrayList<OperatorDeployInfo>(operators.size());
    for (PTOperator node : operators) {
      OperatorDeployInfo ndi = createOperatorDeployInfo(node);
      long checkpointWindowId = node.getRecoveryCheckpoint();
      if (checkpointWindowId > 0) {
        LOG.debug("Operator {} recovery checkpoint {}", node.getId(), Codec.getStringWindowId(checkpointWindowId));
        ndi.checkpointWindowId = checkpointWindowId;
      }
      undeployList.add(ndi);
    }
    return undeployList;
  }

  /**
   * Create deploy info for operator.<p>
   * <br>
   * @param dnodeId
   * @param nodeDecl
   * @return {@link com.malhartech.stram.OperatorDeployInfo}
   *
   */
  private OperatorDeployInfo createOperatorDeployInfo(PTOperator node)
  {
    OperatorDeployInfo ndi = new OperatorDeployInfo();
    Operator operator = node.getOperatorMeta().getOperator();
    ndi.type = (operator instanceof InputOperator) ? OperatorDeployInfo.OperatorType.INPUT : OperatorDeployInfo.OperatorType.GENERIC;
    if (node.merge != null) {
      operator = node.merge;
      ndi.type = OperatorDeployInfo.OperatorType.UNIFIER;
    } else if (node.partition != null) {
      operator = node.partition.getOperator();
    }
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      this.nodeSerDe.write(operator, os);
      ndi.serializedNode = os.toByteArray();
      os.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize " + operator + "(" + operator.getClass() + ")", e);
    }
    ndi.declaredId = node.getOperatorMeta().getId();
    ndi.id = node.getId();
    ndi.contextAttributes = node.getOperatorMeta().getAttributes();
    return ndi;
  }

  public ContainerInfo getContainerInfo() {
    ContainerInfo ci = new ContainerInfo();
    ci.id = container.containerId;
    ci.numOperators = container.operators.size();
    ci.lastHeartbeat = lastHeartbeatMillis;
    return ci;
  }

}

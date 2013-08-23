/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.OperatorDeployInfo.InputDeployInfo;
import com.datatorrent.stram.OperatorDeployInfo.OutputDeployInfo;
import com.datatorrent.stram.PhysicalPlan.PTContainer;
import com.datatorrent.stram.PhysicalPlan.PTInput;
import com.datatorrent.stram.PhysicalPlan.PTOperator;
import com.datatorrent.stram.PhysicalPlan.PTOutput;
import com.datatorrent.stram.PhysicalPlan.PTOperator.State;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.webapp.ContainerInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.bufferserver.util.Codec;

/**
 *
 * Representation of a child container in the master<p>
 * <br>
 *
 * @since 0.3.2
 */
public class StramChildAgent {
  private static final Logger LOG = LoggerFactory.getLogger(StramChildAgent.class);

  public static class ContainerStartRequest {
    final PTContainer container;

    ContainerStartRequest(PTContainer container) {
      this.container = container;
    }
  }

  static class MovingAverageLong {
    private final int periods;
    private final long[] values;
    private int index = 0;
    private boolean filled = false;

    MovingAverageLong(int periods) {
      this.periods = periods;
      this.values = new long[periods];
    }

    void add(long val) {
      values[index++] = val;
      if (index == periods) {
        filled = true;
      }
      index %= periods;
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

  // Generics don't work with numbers.  Hence this mess.
  static class MovingAverageDouble {
    private final int periods;
    private final double[] values;
    private int index = 0;
    private boolean filled = false;

    MovingAverageDouble(int periods) {
      this.periods = periods;
      this.values = new double[periods];
    }

    void add(double val) {
      values[index++] = val;
      if (index == periods) {
        filled = true;
      }
      index %= periods;
    }

    double getAvg() {
      double sum = 0;
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

  protected class OperatorStatus
  {
    StreamingNodeHeartbeat lastHeartbeat;
    final PTOperator operator;
    long totalTuplesProcessed;
    long totalTuplesEmitted;
    long currentWindowId;
    //MovingAverageLong tuplesProcessedPSMA10 = new MovingAverageLong(10);
    //MovingAverageLong tuplesEmittedPSMA10 = new MovingAverageLong(10);
    long tuplesProcessedPSMA10;
    long tuplesEmittedPSMA10;
    MovingAverageDouble cpuPercentageMA10 = new MovingAverageDouble(10);
    MovingAverageLong latencyMA = new MovingAverageLong(10);
    List<String> recordingNames; // null if recording is not in progress
    Map<String, PortStatus> inputPortStatusList = new HashMap<String, PortStatus>();
    Map<String, PortStatus> outputPortStatusList = new HashMap<String, PortStatus>();

    private OperatorStatus(PTOperator operator) {
      this.operator = operator;
      for (PTInput ptInput: operator.inputs) {
        PortStatus inputPortStatus = new PortStatus();
        inputPortStatus.portName = ptInput.portName;
        inputPortStatusList.put(ptInput.portName, inputPortStatus);
      }
      for (PTOutput ptOutput: operator.outputs) {
        PortStatus outputPortStatus = new PortStatus();
        outputPortStatus.portName = ptOutput.portName;
        outputPortStatusList.put(ptOutput.portName, outputPortStatus);
      }
    }

    public boolean isIdle()
    {
      if ((lastHeartbeat != null && DNodeState.IDLE.name().equals(lastHeartbeat.getState()))) {
        return true;
      }
      return false;
    }
  }

  public class PortStatus
  {
    String portName;
    long totalTuples = 0;
    MovingAverageLong tuplesPSMA10 = new MovingAverageLong(10);
    MovingAverageLong bufferServerBytesPSMA10 = new MovingAverageLong(10);  // TBD
  }

  public StramChildAgent(PTContainer container, StreamingContainerContext initCtx, StreamingContainerManager dnmgr) {
    this.container = container;
    this.initCtx = initCtx;
    this.operators = new HashMap<Integer, OperatorStatus>(container.operators.size());
    this.memoryMBFree = this.container.getAllocatedMemoryMB();
    this.dnmgr = dnmgr;
  }

  boolean shutdownRequested = false;
  long lastHeartbeatMillis = 0;
  //long lastCheckpointRequestMillis = 0;
  long createdMillis = System.currentTimeMillis();
  final PTContainer container;
  Map<Integer, OperatorStatus> operators;
  final StreamingContainerContext initCtx;
  Runnable onAck = null;
  String jvmName;
  int memoryMBFree;
  final StreamingContainerManager dnmgr;

  private final ConcurrentLinkedQueue<StramToNodeRequest> operatorRequests = new ConcurrentLinkedQueue<StramToNodeRequest>();

  public StreamingContainerContext getInitContext() {
    return initCtx;
  }

  public boolean hasPendingWork() {
    return this.onAck != null || !container.pendingDeploy.isEmpty() || !container.pendingUndeploy.isEmpty();
  }

  private void ackPendingRequest() {
    if (onAck != null) {
      onAck.run();
      onAck = null;
    }
  }

  protected OperatorStatus updateOperatorStatus(StreamingNodeHeartbeat shb) {
    OperatorStatus status = this.operators.get(shb.getNodeId());
    if (status == null) {
      for (PTOperator operator : container.operators) {
        if (operator.getId() == shb.getNodeId()) {
          status = new OperatorStatus(operator);
          operators.put(shb.getNodeId(), status);
        }
      }
    }

    if (status != null && !container.pendingDeploy.isEmpty()) {
      if (status.operator.getState() == PTOperator.State.PENDING_DEPLOY) {
        // remove operator from deploy list only if not scheduled of undeploy (or redeploy) again
        if (!container.pendingUndeploy.contains(status.operator) && container.pendingDeploy.remove(status.operator)) {
          LOG.debug("{} marking deployed: {} remote status {}", new Object[] {container.containerId, status.operator, shb.getState()});
          status.operator.setState(PTOperator.State.ACTIVE);

          // record started
          HdfsEventRecorder.Event ev = new HdfsEventRecorder.Event("operator-start");
          ev.addData("operatorId", status.operator.getId());
          ev.addData("operatorName", status.operator.getName());
          ev.addData("containerId", container.containerId);
          dnmgr.recordEventAsync(ev);

        }
      }
      LOG.debug("{} pendingDeploy {}", container.containerId, container.pendingDeploy);
    }
    return status;
  }

  public void addOperatorRequest(StramToNodeRequest r) {
    LOG.info("Adding operator request {} {}", container.containerId, r);
    this.operatorRequests.add(r);
  }

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  protected ConcurrentLinkedQueue<StramToNodeRequest> getOperatorRequests() {
    return this.operatorRequests;
  }

  public ContainerHeartbeatResponse pollRequest() {
    ackPendingRequest();

    if (!this.container.pendingUndeploy.isEmpty()) {
      ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
      final Set<PTOperator> toUndeploy = Sets.newHashSet(this.container.pendingUndeploy);
      List<OperatorDeployInfo> nodeList = getUndeployInfoList(toUndeploy);
      rsp.undeployRequest = nodeList;
      rsp.hasPendingRequests = (!this.container.pendingDeploy.isEmpty());
      this.onAck = new Runnable() {
        @Override
        public void run() {
          // remove operators from undeploy list to not request it again
          container.pendingUndeploy.removeAll(toUndeploy);
          long timestamp = System.currentTimeMillis();
          for (PTOperator operator : toUndeploy) {
            operator.setState(PTOperator.State.INACTIVE);

            // record operator stop event
            HdfsEventRecorder.Event ev = new HdfsEventRecorder.Event("operator-stop");
            ev.addData("operatorId", operator.getId());
            ev.addData("containerId", operator.container.containerId);
            ev.addData("reason", "undeploy");
            ev.setTimestamp(timestamp);
            StramChildAgent.this.dnmgr.recordEventAsync(ev);
          }
          LOG.debug("{} undeploy complete: {} deploy: {}", new Object[] {container.containerId, toUndeploy, container.pendingDeploy});
        }
      };
      return rsp;
    }

    if (!this.container.pendingDeploy.isEmpty()) {
      Set<PTOperator> deployOperators = this.container.plan.getOperatorsForDeploy(this.container);
      LOG.debug("container {} deployable operators: {}", container.containerId, deployOperators);
      ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
      List<OperatorDeployInfo> deployList = getDeployInfoList(deployOperators);
      if (deployList != null && !deployList.isEmpty()) {
        rsp.deployRequest = deployList;
        rsp.nodeRequests = Lists.newArrayList();
        for (PTOperator o : deployOperators) {
          rsp.nodeRequests.addAll(o.deployRequests);
        }
      }
      rsp.hasPendingRequests = false;
      return rsp;
    }

    return null;
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

  // this method is only used for testing
  public List<OperatorDeployInfo> getDeployInfo() {
    return getDeployInfoList(container.pendingDeploy);
  }

  /**
   * Create deploy info for StramChild.
   * @param operators
   * @return StreamingContainerContext
   */
  private List<OperatorDeployInfo> getDeployInfoList(Set<PTOperator> operators) {

    if (container.bufferServerAddress == null) {
      throw new IllegalStateException("No buffer server address assigned");
    }

    Map<OperatorDeployInfo, PTOperator> nodes = new LinkedHashMap<OperatorDeployInfo, PTOperator>();
    Map<String, OutputDeployInfo> publishers = new LinkedHashMap<String, OutputDeployInfo>();

    for (PTOperator node : operators) {
      if (node.getState() != State.NEW && node.getState() != State.INACTIVE) {
        LOG.debug("Skipping deploy for operator {} state {}", node, node.getState());
        continue;
      }
      node.setState(State.PENDING_DEPLOY);
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
        portInfo.contextAttributes =streamMeta.getSource().getAttributes();

        if (ndi.type == OperatorDeployInfo.OperatorType.UNIFIER) {
          // input attributes of the downstream operator
          for (InputPortMeta sink : streamMeta.getSinks()) {
            portInfo.contextAttributes = sink.getAttributes();
            break;
          }
        }

        if (!out.isDownStreamInline()) {
          portInfo.bufferServerHost = node.container.bufferServerAddress.getHostName();
          portInfo.bufferServerPort = node.container.bufferServerAddress.getPort();
          if (streamMeta.getCodecClass() != null) {
            portInfo.serDeClassName = streamMeta.getCodecClass().getName();
          }
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

        if (inputInfo.contextAttributes == null && ndi.type == OperatorDeployInfo.OperatorType.UNIFIER) {
          inputInfo.contextAttributes = in.source.logicalStream.getSource().getAttributes();
        }

        inputInfo.sourceNodeId = sourceOutput.source.getId();
        inputInfo.sourcePortName = sourceOutput.portName;
        if (in.partitions != null && in.partitions.mask != 0) {
          inputInfo.partitionMask = in.partitions.mask;
          inputInfo.partitionKeys = in.partitions.partitions;
        }

        if (sourceOutput.source.container == node.container) {
          // inline input (both operators in same container)
          OutputDeployInfo outputInfo = publishers.get(sourceOutput.source.getId() + "/" + streamMeta.getId());
          if (outputInfo == null) {
            throw new AssertionError("Missing publisher for inline stream " + sourceOutput);
          }
        } else {
          // buffer server input
          InetSocketAddress addr = sourceOutput.source.container.bufferServerAddress;
          if (addr == null) {
            throw new AssertionError("upstream address not assigned: " + sourceOutput);
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
  private List<OperatorDeployInfo> getUndeployInfoList(Set<PTOperator> operators) {
    List<OperatorDeployInfo> undeployList = new ArrayList<OperatorDeployInfo>(operators.size());
    for (PTOperator node : operators) {
      node.setState(State.PENDING_UNDEPLOY);
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
   * Create deploy info for operator.
   * <p>
   *
   * @param dnodeId
   * @param nodeDecl
   * @return {@link com.datatorrent.stram.OperatorDeployInfo}
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
    StorageAgent agent = node.getOperatorMeta().getAttributes().attr(OperatorContext.STORAGE_AGENT).get();
    if (agent == null) {
      String appPath = getInitContext().attrValue(LogicalPlan.APPLICATION_PATH, "app-dfs-path-not-configured");
      agent = new HdfsStorageAgent(new Configuration(), appPath + "/" + LogicalPlan.SUBDIR_CHECKPOINTS);
    }

    try {
      OutputStream stream = agent.getSaveStream(node.getId(), -1);
      Node.storeOperator(stream, operator);
      stream.close();
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to serialize and store " + operator + "(" + operator.getClass() + ")", e);
    }
    ndi.declaredId = node.getOperatorMeta().getName();
    ndi.id = node.getId();
    ndi.contextAttributes = node.getOperatorMeta().getAttributes();
    return ndi;
  }

  public ContainerInfo getContainerInfo() {
    ContainerInfo ci = new ContainerInfo();
    ci.id = container.containerId;
    ci.host = container.host;
    ci.state = container.getState().name();
    ci.jvmName = this.jvmName;
    ci.numOperators = container.operators.size();
    ci.memoryMBAllocated = container.getAllocatedMemoryMB();
    ci.lastHeartbeat = lastHeartbeatMillis;
    ci.memoryMBFree = this.memoryMBFree;
    return ci;
  }

}

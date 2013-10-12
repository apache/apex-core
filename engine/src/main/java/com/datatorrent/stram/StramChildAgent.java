/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.api.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context.PortContext;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.StorageAgent;

import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.stram.OperatorDeployInfo.InputDeployInfo;
import com.datatorrent.stram.OperatorDeployInfo.OperatorType;
import com.datatorrent.stram.OperatorDeployInfo.OutputDeployInfo;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat.DNodeState;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.Stats;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PTOperator.State;
import com.datatorrent.stram.webapp.ContainerInfo;

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

  static class TimedMovingAverageLong {
    private final int periods;
    private final long[] values;
    private final long[] timeIntervals;
    private int index = 0;
    private final long baseTime;

    TimedMovingAverageLong(int periods, long baseTime) {
      this.periods = periods;
      this.values = new long[periods];
      this.timeIntervals = new long[periods];
      this.baseTime = baseTime;
    }

    void add(long val, long time) {
      values[index] = val;
      timeIntervals[index] = time;
      index++;
      index %= periods;
    }

    double getAvg() {
      long sumValues = 0;
      long sumTimeIntervals = 0;
      int i = index;
      while (true) {
        i--;
        if (i < 0) {
          i = periods - 1;
        }
        if (i == index) {
          break;
        }
        sumValues += values[i];
        sumTimeIntervals += timeIntervals[i];
        if (sumTimeIntervals >= baseTime) {
          break;
        }
      }

      if (sumTimeIntervals == 0) {
        return 0;
      }
      else {
        return ((double)sumValues * 1000) / sumTimeIntervals;
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
    long tuplesProcessedPSMA10;
    long tuplesEmittedPSMA10;
    long recordingStartTime = Stats.INVALID_TIME_MILLIS;
    MovingAverageDouble cpuPercentageMA10 = new MovingAverageDouble(10);
    MovingAverageLong latencyMA = new MovingAverageLong(10);
    List<String> recordingNames; // null if recording is not in progress
    Map<String, PortStatus> inputPortStatusList = new HashMap<String, PortStatus>();
    Map<String, PortStatus> outputPortStatusList = new HashMap<String, PortStatus>();

    private OperatorStatus(PTOperator operator) {
      this.operator = operator;
      for (PTOperator.PTInput ptInput: operator.getInputs()) {
        PortStatus inputPortStatus = new PortStatus();
        inputPortStatus.portName = ptInput.portName;
        inputPortStatusList.put(ptInput.portName, inputPortStatus);
      }
      for (PTOperator.PTOutput ptOutput: operator.getOutputs()) {
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
    long totalTuples;
    long recordingStartTime = Stats.INVALID_TIME_MILLIS;
    TimedMovingAverageLong tuplesPSMA10 = new TimedMovingAverageLong(1000, 10000);
    TimedMovingAverageLong bufferServerBytesPSMA10 = new TimedMovingAverageLong(1000, 10000);  // TBD
  }

  public StramChildAgent(PTContainer container, StreamingContainerContext initCtx, StreamingContainerManager dnmgr) {
    this.container = container;
    this.initCtx = initCtx;
    this.operators = new HashMap<Integer, OperatorStatus>(container.getOperators().size());
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
    return this.onAck != null || !container.getPendingDeploy().isEmpty() || !container.getPendingUndeploy().isEmpty();
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
      for (PTOperator operator : container.getOperators()) {
        if (operator.getId() == shb.getNodeId()) {
          status = new OperatorStatus(operator);
          operators.put(shb.getNodeId(), status);
        }
      }
    }

    if (status != null && !container.getPendingDeploy().isEmpty()) {
      if (status.operator.getState() == PTOperator.State.PENDING_DEPLOY) {
        // remove operator from deploy list only if not scheduled of undeploy (or redeploy) again
        if (!container.getPendingUndeploy().contains(status.operator) && container.getPendingDeploy().remove(status.operator)) {
          LOG.debug("{} marking deployed: {} remote status {}", new Object[] {container.getExternalId(), status.operator, shb.getState()});
          status.operator.setState(PTOperator.State.ACTIVE);

          // record started
          HdfsEventRecorder.Event ev = new HdfsEventRecorder.Event("operator-start");
          ev.addData("operatorId", status.operator.getId());
          ev.addData("operatorName", status.operator.getName());
          ev.addData("containerId", container.getExternalId());
          dnmgr.recordEventAsync(ev);
        }
      }
      LOG.debug("{} pendingDeploy {}", container.getExternalId(), container.getPendingDeploy());
    }
    return status;
  }

  public void addOperatorRequest(StramToNodeRequest r) {
    LOG.info("Adding operator request {} {}", container.getExternalId(), r);
    this.operatorRequests.add(r);
  }

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  protected ConcurrentLinkedQueue<StramToNodeRequest> getOperatorRequests() {
    return this.operatorRequests;
  }

  public ContainerHeartbeatResponse pollRequest() {
    ackPendingRequest();

    if (!this.container.getPendingUndeploy().isEmpty()) {
      ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
      final Set<PTOperator> toUndeploy = Sets.newHashSet(this.container.getPendingUndeploy());
      List<OperatorDeployInfo> nodeList = getUndeployInfoList(toUndeploy);
      rsp.undeployRequest = nodeList;
      rsp.hasPendingRequests = (!this.container.getPendingDeploy().isEmpty());
      this.onAck = new Runnable() {
        @Override
        public void run() {
          // remove operators from undeploy list to not request it again
          container.getPendingUndeploy().removeAll(toUndeploy);
          long timestamp = System.currentTimeMillis();
          for (PTOperator operator : toUndeploy) {
            operator.setState(PTOperator.State.INACTIVE);

            // record operator stop event
            HdfsEventRecorder.Event ev = new HdfsEventRecorder.Event("operator-stop");
            ev.addData("operatorId", operator.getId());
            ev.addData("containerId", operator.getContainer().getExternalId());
            ev.addData("reason", "undeploy");
            ev.setTimestamp(timestamp);
            StramChildAgent.this.dnmgr.recordEventAsync(ev);
          }
          LOG.debug("{} undeploy complete: {} deploy: {}", new Object[] {container.getExternalId(), toUndeploy, container.getPendingDeploy()});
        }
      };
      return rsp;
    }

    if (!this.container.getPendingDeploy().isEmpty()) {
      Set<PTOperator> deployOperators = this.container.getPlan().getOperatorsForDeploy(this.container);
      LOG.debug("container {} deployable operators: {}", container.getExternalId(), deployOperators);
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
    return getDeployInfoList(container.getPendingDeploy());
  }

  /**
   * Create deploy info for StramChild.
   * @param operators
   * @return StreamingContainerContext
   */
  private List<OperatorDeployInfo> getDeployInfoList(Set<PTOperator> operators) {

    if (container.bufferServerAddress == null) {
      throw new AssertionError("No buffer server address assigned");
    }

    Map<OperatorDeployInfo, PTOperator> nodes = new LinkedHashMap<OperatorDeployInfo, PTOperator>();
    Map<String, OutputDeployInfo> publishers = new LinkedHashMap<String, OutputDeployInfo>();

    for (PTOperator oper : operators) {
      if (oper.getState() != State.NEW && oper.getState() != State.INACTIVE) {
        LOG.debug("Skipping deploy for operator {} state {}", oper, oper.getState());
        continue;
      }
      oper.setState(State.PENDING_DEPLOY);
      OperatorDeployInfo ndi = createOperatorDeployInfo(oper);

      nodes.put(ndi, oper);
      ndi.inputs = new ArrayList<InputDeployInfo>(oper.getInputs().size());
      ndi.outputs = new ArrayList<OutputDeployInfo>(oper.getOutputs().size());

      for (PTOperator.PTOutput out : oper.getOutputs()) {
        final StreamMeta streamMeta = out.logicalStream;
        // buffer server or inline publisher
        OutputDeployInfo portInfo = new OutputDeployInfo();
        portInfo.declaredStreamId = streamMeta.getId();
        portInfo.portName = out.portName;

        streamMeta.getSource().getAttributes();
        portInfo.contextAttributes =streamMeta.getSource().getAttributes();

        // Need to create one attribute list tha will contain both context and runtime attributes in which case a new attribute list
        // needs to be created instead of all instances sharing a common one from logical. This will be done when new attribute API
        // is ready
        portInfo.runtimeAttributes = new DefaultAttributeMap(com.datatorrent.api.Context.PortContext.class);

        boolean outputUnified = false;
        for (PTOperator.PTInput input : out.sinks) {
          if (input.target.getUnifier() != null) {
            outputUnified = true;
            break;
          }
        }
        portInfo.runtimeAttributes.attr(PortContext.IS_OUTPUT_UNIFIED).set(outputUnified);

        if (ndi.type == OperatorDeployInfo.OperatorType.UNIFIER) {
          // input attributes of the downstream operator
          for (InputPortMeta sink : streamMeta.getSinks()) {
            portInfo.contextAttributes = sink.getAttributes();
            break;
          }
        }

        if (!out.isDownStreamInline()) {
          portInfo.bufferServerHost = oper.getContainer().bufferServerAddress.getHostName();
          portInfo.bufferServerPort = oper.getContainer().bufferServerAddress.getPort();
          if (streamMeta.getCodecClass() != null) {
            portInfo.serDeClassName = streamMeta.getCodecClass().getName();
          }
        }

        ndi.outputs.add(portInfo);
        publishers.put(oper.getId() + "/" + streamMeta.getId(), portInfo);
      }
    }

    // after we know all publishers within container, determine subscribers

    for (Map.Entry<OperatorDeployInfo, PTOperator> operEntry : nodes.entrySet()) {
      OperatorDeployInfo ndi = operEntry.getKey();
      PTOperator oper = operEntry.getValue();
      for (PTOperator.PTInput in : oper.getInputs()) {
        final StreamMeta streamMeta = in.logicalStream;
        if (streamMeta.getSource() == null) {
          throw new AssertionError("source is null: " + in);
        }
        PTOperator.PTOutput sourceOutput = in.source;

        InputDeployInfo inputInfo = new InputDeployInfo();
        inputInfo.declaredStreamId = streamMeta.getId();
        inputInfo.portName = in.portName;
        for (Map.Entry<InputPortMeta, StreamMeta> e : oper.getOperatorMeta().getInputStreams().entrySet()) {
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

        if (sourceOutput.source.getContainer() == oper.getContainer()) {
          // both operators in same container
          OutputDeployInfo outputInfo = publishers.get(sourceOutput.source.getId() + "/" + streamMeta.getId());
          if (outputInfo == null) {
            throw new AssertionError("No publisher for inline stream " + sourceOutput);
          }
          if (streamMeta.getLocality() == Locality.THREAD_LOCAL && oper.getInputs().size() == 1) {
            inputInfo.locality = Locality.THREAD_LOCAL;
            ndi.type = OperatorType.OIO;
          } else {
            inputInfo.locality = Locality.CONTAINER_LOCAL;
          }

        } else {
          // buffer server input
          InetSocketAddress addr = sourceOutput.source.getContainer().bufferServerAddress;
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
      node.setState(PTOperator.State.PENDING_UNDEPLOY);
      OperatorDeployInfo ndi = createOperatorDeployInfo(node);
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
  private OperatorDeployInfo createOperatorDeployInfo(PTOperator oper)
  {
    OperatorDeployInfo ndi = new OperatorDeployInfo();
    Operator operator = oper.getOperatorMeta().getOperator();
    ndi.type = (operator instanceof InputOperator) ? OperatorDeployInfo.OperatorType.INPUT : OperatorDeployInfo.OperatorType.GENERIC;
    if (oper.getUnifier() != null) {
      operator = oper.getUnifier();
      ndi.type = OperatorDeployInfo.OperatorType.UNIFIER;
    } else if (oper.getPartition() != null) {
      operator = oper.getPartition().getOperator();
    }

    long checkpointWindowId = oper.getRecoveryCheckpoint();
    ProcessingMode pm = oper.getOperatorMeta().attrValue(OperatorContext.PROCESSING_MODE, null);

    if (checkpointWindowId == 0 || pm == ProcessingMode.AT_MOST_ONCE || pm == ProcessingMode.EXACTLY_ONCE) {
      StorageAgent agent = oper.getOperatorMeta().getAttributes().attr(OperatorContext.STORAGE_AGENT).get();
      if (agent == null) {
        String appPath = getInitContext().attrValue(LogicalPlan.APPLICATION_PATH, "app-dfs-path-not-configured");
        agent = new HdfsStorageAgent(new Configuration(), appPath + "/" + LogicalPlan.SUBDIR_CHECKPOINTS);
      }
      // pick the checkpoint most recently written to HDFS
      try {
        checkpointWindowId = agent.getMostRecentWindowId(oper.getId());
      }
      catch (Exception e) {
        try {
          OutputStream stream = agent.getSaveStream(oper.getId(), OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID);
          Node.storeOperator(stream, operator);
          stream.close();
          checkpointWindowId = OperatorDeployInfo.STATELESS_CHECKPOINT_WINDOW_ID;
        }
        catch (IOException io) {
          throw new RuntimeException("Failed to access checkpoint state " + operator + "(" + operator.getClass() + ")", e);
        }
      }
    }

    LOG.debug("Operator {} recovery checkpoint {}", oper, Codec.getStringWindowId(checkpointWindowId));
    ndi.checkpointWindowId = checkpointWindowId;
    ndi.declaredId = oper.getOperatorMeta().getName();
    ndi.id = oper.getId();
    ndi.contextAttributes = oper.getOperatorMeta().getAttributes();

    return ndi;
  }

  public ContainerInfo getContainerInfo() {
    ContainerInfo ci = new ContainerInfo();
    ci.id = container.getExternalId();
    ci.host = container.host;
    ci.state = container.getState().name();
    ci.jvmName = this.jvmName;
    ci.numOperators = container.getOperators().size();
    ci.memoryMBAllocated = container.getAllocatedMemoryMB();
    ci.lastHeartbeat = lastHeartbeatMillis;
    ci.memoryMBFree = this.memoryMBFree;
    return ci;
  }

}

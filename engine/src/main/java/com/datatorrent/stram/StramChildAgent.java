/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PTOperator.State;
import com.datatorrent.stram.webapp.ContainerInfo;

/**
 *
 * Representation of child container (execution layer) in the master<p>
 * Created when resource for container was allocated.
 * Destroyed after resource is deallocated (container released, killed etc.)
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


  public StramChildAgent(PTContainer container, StreamingContainerContext initCtx, StreamingContainerManager dnmgr) {
    this.container = container;
    this.initCtx = initCtx;
    this.operators = Maps.newHashMapWithExpectedSize(container.getOperators().size());
    this.memoryMBFree = this.container.getAllocatedMemoryMB();
    this.dnmgr = dnmgr;
  }

  boolean shutdownRequested = false;
  long lastHeartbeatMillis = 0;
  long createdMillis = System.currentTimeMillis();
  final PTContainer container;
  final Map<Integer, PTOperator> operators;
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

  protected PTOperator updateOperatorStatus(OperatorHeartbeat shb) {
    PTOperator oper = this.operators.get(shb.getNodeId());
    // index the operator for future access
    if (oper == null) {
      for (PTOperator operator : container.getOperators()) {
        if (operator.getId() == shb.getNodeId()) {
          oper = operator;
          operators.put(shb.getNodeId(), oper);
        }
      }
    }
    if (oper != null && !container.getPendingDeploy().isEmpty()) {
      if (oper.getState() == PTOperator.State.PENDING_DEPLOY) {
        // remove operator from deploy list only if not scheduled of undeploy (or redeploy) again
        if (!container.getPendingUndeploy().contains(oper) && container.getPendingDeploy().remove(oper)) {
          LOG.debug("{} marking deployed: {} remote status {}", new Object[] {container.getExternalId(), oper, shb.getState()});
          oper.setState(PTOperator.State.ACTIVE);

          // record started
          FSEventRecorder.Event ev = new FSEventRecorder.Event("operator-start");
          ev.addData("operatorId", oper.getId());
          ev.addData("operatorName", oper.getName());
          ev.addData("containerId", container.getExternalId());
          dnmgr.recordEventAsync(ev);
        }
      }
      LOG.debug("{} pendingDeploy {}", container.getExternalId(), container.getPendingDeploy());
    }
    return oper;
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
            FSEventRecorder.Event ev = new FSEventRecorder.Event("operator-stop");
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
    for (PTOperator oper : this.operators.values()) {
      if (!oper.stats.isIdle()) {
        return false;
      }
    }
    return true;
  }

  // this method is only used for testing
  @VisibleForTesting
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
        portInfo.contextAttributes =streamMeta.getSource().getAttributes();

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
          if (streamMeta.getLocality() == Locality.THREAD_LOCAL) {
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
    ProcessingMode pm = oper.getOperatorMeta().getValue(OperatorContext.PROCESSING_MODE);

    if (checkpointWindowId == 0 || pm == ProcessingMode.AT_MOST_ONCE || pm == ProcessingMode.EXACTLY_ONCE) {
      StorageAgent agent = oper.getOperatorMeta().getAttributes().get(OperatorContext.STORAGE_AGENT);
      if (agent == null) {
        String appPath = getInitContext().getValue(LogicalPlan.APPLICATION_PATH);
        agent = new FSStorageAgent(new Configuration(), appPath + "/" + LogicalPlan.SUBDIR_CHECKPOINTS);
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

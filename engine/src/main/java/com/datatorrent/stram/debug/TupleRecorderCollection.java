/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.debug;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Sink;

import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat;
import com.datatorrent.stram.api.ContainerContext;
import com.datatorrent.stram.api.NodeActivationListener;
import com.datatorrent.stram.api.NodeRequest;
import com.datatorrent.stram.api.NodeRequest.RequestType;
import com.datatorrent.stram.api.RequestFactory;
import com.datatorrent.stram.api.RequestFactory.RequestDelegate;
import com.datatorrent.stram.api.StatsListener.ContainerStatsListener;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.Stats;
import com.datatorrent.stram.engine.Stats.ContainerStats;
import com.datatorrent.stram.engine.Stats.ContainerStats.OperatorStats;
import com.datatorrent.stram.engine.Stats.ContainerStats.OperatorStats.PortStats;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;
import com.datatorrent.stram.plan.logical.Operators.PortMappingDescriptor;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient;

/**
 * <p>TupleRecorderCollection class.</p>
 *
 * @since 0.3.5
 */
public class TupleRecorderCollection extends HashMap<OperatorIdPortNamePair, TupleRecorder> implements Component<Context>, NodeActivationListener, ContainerStatsListener
{
  private int tupleRecordingPartFileSize;
  private String gatewayAddress;
  private long tupleRecordingPartFileTimeMillis;
  private String appPath;
  private String containerId; // this should be retired!
  private SharedPubSubWebSocketClient wsClient;

  public TupleRecorder getTupleRecorder(int operId, String portName)
  {
    //logger.debug("attempting to get tuple recorder for {} on {}", new OperatorIdPortNamePair(operId, portName), System.identityHashCode(this));
    return get(new OperatorIdPortNamePair(operId, portName));
  }

  @Override
  public void setup(Context ctx)
  {
    tupleRecordingPartFileSize = ctx.getValue(LogicalPlan.TUPLE_RECORDING_PART_FILE_SIZE);
    tupleRecordingPartFileTimeMillis = ctx.getValue(LogicalPlan.TUPLE_RECORDING_PART_FILE_TIME_MILLIS);
    containerId = ctx.getValue(ContainerContext.IDENTIFIER);
    gatewayAddress = ctx.getValue(LogicalPlan.GATEWAY_ADDRESS);
    appPath = ctx.getValue(LogicalPlan.APPLICATION_PATH);

    RequestDelegateImpl impl = new RequestDelegateImpl();
    RequestFactory rf = ctx.getValue(ContainerContext.REQUEST_FACTORY);
    if (rf == null) {
      logger.warn("No request factory defined, recording is disabled!");
    }
    else {
      rf.registerDelegate(RequestType.START_RECORDING, impl);
      rf.registerDelegate(RequestType.STOP_RECORDING, impl);
      rf.registerDelegate(RequestType.SYNC_RECORDING, impl);
    }
    if (gatewayAddress != null) {
      try {
        wsClient = new SharedPubSubWebSocketClient("ws://" + gatewayAddress + "/pubsub", 500);
      }
      catch (Exception ex) {
        logger.warn("Error initializing websocket", ex);
      }
    }
  }

  @Override
  public void teardown()
  {
    for (TupleRecorder entry : values()) {
      entry.teardown();
    }
    clear();
  }

  /**
   * Return the name of the stream which is operatorId.portname.
   *
   * @param operatorId id of the operator to which the port belongs.
   * @param portname name of port to which the stream is connected.
   * @return Stream Id if connected, null otherwise.
   */
  public final String getDeclaredStreamId(int operatorId, String portname)
  {
    return String.valueOf(operatorId).concat(Component.CONCAT_SEPARATOR).concat(portname);
  }

  private void startRecording(Node<?> node, int operatorId, String portName)
  {
    PortMappingDescriptor descriptor = node.getPortMappingDescriptor();
    OperatorIdPortNamePair operatorIdPortNamePair = new OperatorIdPortNamePair(operatorId, portName);
    // check any recording conflict
    boolean conflict = false;
    if (containsKey(new OperatorIdPortNamePair(operatorId, null))) {
      conflict = true;
    }
    else if (portName == null) {
      for (Map.Entry<String, PortContextPair<InputPort<?>>> entry : descriptor.inputPorts.entrySet()) {
        if (containsKey(new OperatorIdPortNamePair(operatorId, entry.getKey()))) {
          conflict = true;
          break;
        }
      }
      for (Map.Entry<String, PortContextPair<OutputPort<?>>> entry : descriptor.outputPorts.entrySet()) {
        if (containsKey(new OperatorIdPortNamePair(operatorId, entry.getKey()))) {
          conflict = true;
          break;
        }
      }
    }
    else {
      if (containsKey(operatorIdPortNamePair)) {
        conflict = true;
      }
    }
    if (!conflict) {
      logger.debug("Executing start recording request for {}", operatorIdPortNamePair);

      TupleRecorder tupleRecorder = new TupleRecorder();
      tupleRecorder.setContainerId(containerId);
      tupleRecorder.setWebSocketClient(wsClient);

      HashMap<String, Sink<Object>> sinkMap = new HashMap<String, Sink<Object>>();
      for (Map.Entry<String, PortContextPair<InputPort<?>>> entry : descriptor.inputPorts.entrySet()) {
        String streamId = getDeclaredStreamId(operatorId, entry.getKey());
        if (streamId == null) {
          streamId = portName + "_implicit_stream";
        }
        if (entry.getValue().context != null && (portName == null || entry.getKey().equals(portName))) {
          logger.debug("Adding recorder sink to input port {}, stream {}", entry.getKey(), streamId);
          tupleRecorder.addInputPortInfo(entry.getKey(), streamId);
          sinkMap.put(entry.getKey(), tupleRecorder.newSink(entry.getKey()));
        }
      }
      for (Map.Entry<String, PortContextPair<OutputPort<?>>> entry : descriptor.outputPorts.entrySet()) {
        String streamId = getDeclaredStreamId(operatorId, entry.getKey());
        if (streamId == null) {
          streamId = portName + "_implicit_stream";
        }
        if (portName == null || entry.getKey().equals(portName)) {
          logger.debug("Adding recorder sink to output port {}, stream {}", entry.getKey(), streamId);
          tupleRecorder.addOutputPortInfo(entry.getKey(), streamId);
          sinkMap.put(entry.getKey(), tupleRecorder.newSink(entry.getKey()));
        }
      }
      if (!sinkMap.isEmpty()) {
        logger.debug("Started recording on {} through {}", operatorIdPortNamePair, System.identityHashCode(this));
        String basePath = appPath + "/recordings/" + operatorId + "/" + tupleRecorder.getStartTime();
        tupleRecorder.getStorage().setBasePath(basePath);
        tupleRecorder.getStorage().setBytesPerPartFile(tupleRecordingPartFileSize);
        tupleRecorder.getStorage().setMillisPerPartFile(tupleRecordingPartFileTimeMillis);

        // this is not needed, and should be deleted when UI upgrades
        if (portName == null) {
          tupleRecorder.setRecordingName(containerId.concat("_").concat(String.valueOf(operatorId)).concat("_").concat(String.valueOf(tupleRecorder.getStartTime())));
        }
        else {
          tupleRecorder.setRecordingName(containerId.concat("_").concat(String.valueOf(operatorId)).concat("$").concat(portName).concat("_").concat(String.valueOf(tupleRecorder.getStartTime())));
        }

        node.addSinks(sinkMap);
        tupleRecorder.setup(node.getOperator());
        put(operatorIdPortNamePair, tupleRecorder);
      }
      else {
        logger.warn("Tuple recording request ignored because operator is not connected on the specified port.");
      }
    }
    else {
      logger.error("Operator id {} is already being recorded.", operatorId);
    }
  }

  private void stopRecording(Node<?> node, int operatorId, String portName)
  {
    OperatorIdPortNamePair operatorIdPortNamePair = new OperatorIdPortNamePair(operatorId, portName);
    if (containsKey(operatorIdPortNamePair)) {
      logger.debug("Executing stop recording request for {}", operatorIdPortNamePair);
      TupleRecorder tupleRecorder = get(operatorIdPortNamePair);
      if (tupleRecorder != null) {
        node.removeSinks(tupleRecorder.getSinkMap());
        tupleRecorder.teardown();
        logger.debug("Stopped recording for {}", operatorIdPortNamePair);
        remove(operatorIdPortNamePair);
      }
    }
    // this should be looked at again when we redesign how we handle recordings with ports in a cleaner way
    else if (portName == null) {
      Iterator<Map.Entry<OperatorIdPortNamePair, TupleRecorder>> iterator = entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<OperatorIdPortNamePair, TupleRecorder> entry = iterator.next();
        if (operatorId == entry.getKey().operatorId) {
          TupleRecorder tupleRecorder = entry.getValue();
          if (tupleRecorder != null) {
            node.removeSinks(tupleRecorder.getSinkMap());
            tupleRecorder.teardown();
            logger.debug("Stopped recording for operator/port {}", operatorIdPortNamePair);
            iterator.remove();
          }
        }
      }
    }
    else {
      logger.error("Operator/port {} is not being recorded.", operatorIdPortNamePair);
    }
  }

  private void syncRecording(Node<?> node, int operatorId, String portName)
  {
    OperatorIdPortNamePair operatorIdPortNamePair = new OperatorIdPortNamePair(operatorId, portName);
    if (containsKey(operatorIdPortNamePair)) {
      logger.debug("Executing sync recording request for {}", operatorIdPortNamePair);

      TupleRecorder tupleRecorder = get(operatorIdPortNamePair);
      if (tupleRecorder != null) {
        tupleRecorder.getStorage().requestSync();
        logger.debug("Requested sync recording for operator/port {}", operatorIdPortNamePair);
      }
    }
    // this should be looked at again when we redesign how we handle recordings with ports in a cleaner way
    else if (portName == null) {
      Iterator<Map.Entry<OperatorIdPortNamePair, TupleRecorder>> iterator = entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<OperatorIdPortNamePair, TupleRecorder> entry = iterator.next();
        if (operatorId == entry.getKey().operatorId) {
          TupleRecorder tupleRecorder = entry.getValue();
          if (tupleRecorder != null) {
            tupleRecorder.getStorage().requestSync();
            logger.debug("Requested sync recording for operator/port {}", operatorIdPortNamePair);
          }
        }
      }
    }
    else {
      logger.error("(SYNC_RECORDING) Operator/port {} is not being recorded.", operatorIdPortNamePair);
    }
  }

  @Override
  public void activated(Node<?> node)
  {
    for (Map.Entry<String, PortContextPair<InputPort<?>>> entry : node.getPortMappingDescriptor().inputPorts.entrySet()) {
      if (entry.getValue().context != null && entry.getValue().context.getValue(PortContext.AUTO_RECORD)) {
        startRecording(node, node.getId(), entry.getKey());
      }
    }

    for (Map.Entry<String, PortContextPair<OutputPort<?>>> entry : node.getPortMappingDescriptor().outputPorts.entrySet()) {
      if (entry.getValue().context != null && entry.getValue().context.getValue(PortContext.AUTO_RECORD)) {
        startRecording(node, node.getId(), entry.getKey());
      }
    }
  }

  @Override
  public void deactivated(Node<?> node)
  {
    stopRecording(node, node.getId(), null);
  }

  @Override
  public void collected(ContainerStats stats)
  {
    for (StreamingNodeHeartbeat node : stats.nodes) {
      long recordingStartTime;
      TupleRecorder tupleRecorder = get(new OperatorIdPortNamePair(node.nodeId, null));
      if (tupleRecorder == null) {
        recordingStartTime = Stats.INVALID_TIME_MILLIS;
        for (Map.Entry<OperatorIdPortNamePair, TupleRecorder> entry : this.entrySet()) {
          if (entry.getKey().operatorId == node.nodeId) {
            for (OperatorStats os : node.windowStats) {
              if (os.inputPorts != null) {
                for (PortStats ps : os.inputPorts) {
                  if (ps.id.equals(entry.getKey().portName)) {
                    ps.recordingStartTime = entry.getValue().getStartTime();
                  }
                  else {
                    ps.recordingStartTime = Stats.INVALID_TIME_MILLIS;
                  }
                }
              }
              if (os.outputPorts != null) {
                for (PortStats ps : os.outputPorts) {
                  if (ps.id.equals(entry.getKey().portName)) {
                    ps.recordingStartTime = entry.getValue().getStartTime();
                  }
                  else {
                    ps.recordingStartTime = Stats.INVALID_TIME_MILLIS;
                  }
                }
              }
            }
          }
        }
      }
      else {
        recordingStartTime = tupleRecorder.getStartTime();
      }

      for (OperatorStats os : node.windowStats) {
        os.recordingStartTime = recordingStartTime;
      }
    }
  }

  private class RequestDelegateImpl implements RequestDelegate
  {
    @Override
    public NodeRequest getRequestExecutor(final Node<?> node, final StramToNodeRequest snr)
    {
      switch (snr.getRequestType()) {
        case START_RECORDING:
          return new NodeRequest()
          {
            @Override
            public void execute(Operator operator, int operatorId, long windowId) throws IOException
            {
              startRecording(node, operatorId, snr.getPortName());
            }

            @Override
            public String toString()
            {
              return "Start Recording";
            }

          };

        case STOP_RECORDING:
          return new NodeRequest()
          {
            @Override
            public void execute(Operator operator, int operatorId, long windowId) throws IOException
            {
              stopRecording(node, operatorId, snr.getPortName());
            }

            @Override
            public String toString()
            {
              return "Stop Recording";
            }

          };

        case SYNC_RECORDING:
          return new NodeRequest()
          {
            @Override
            public void execute(Operator operator, int operatorId, long windowId) throws IOException
            {
              syncRecording(node, operatorId, snr.getPortName());
            }

            @Override
            public String toString()
            {
              return "Recording Request";
            }

          };

        default:
          throw new UnsupportedOperationException("Unknown request type " + snr.requestType);
      }
    }

  }

  private static final long serialVersionUID = 201309112123L;
  private static final Logger logger = LoggerFactory.getLogger(TupleRecorderCollection.class);
}

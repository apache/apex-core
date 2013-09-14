/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.debug;

import java.io.IOException;
import java.net.URISyntaxException;
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

import com.datatorrent.stram.RequestFactory;
import com.datatorrent.stram.RequestFactory.RequestDelegate;
import com.datatorrent.stram.StramChild;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.TupleRecorder;
import com.datatorrent.stram.api.NodeActivationListener;
import com.datatorrent.stram.api.NodeRequest;
import com.datatorrent.stram.api.NodeRequest.RequestType;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;
import com.datatorrent.stram.plan.logical.Operators.PortMappingDescriptor;

public class TupleRecorderCollection extends HashMap<String, TupleRecorder> implements Component<Context>, NodeActivationListener
{
  private int tupleRecordingPartFileSize;
  private String daemonAddress;
  private long tupleRecordingPartFileTimeMillis;
  private String appPath;

  public TupleRecorder getTupleRecorder(int operId, String portName)
  {
    return get(getRecorderKey(operId, portName));
  }

  private static String getRecorderKey(int operatorId, String portName)
  {
    if (portName == null) {
      return String.valueOf(operatorId);
    }
    else {
      return String.valueOf(operatorId).concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(portName);
    }
  }

  private static String getOperatorFromRecorderKey(String recorderKey)
  {
    if (recorderKey.contains(StramChild.NODE_PORT_CONCAT_SEPARATOR)) {
      return recorderKey.substring(0, recorderKey.indexOf(StramChild.NODE_PORT_CONCAT_SEPARATOR));
    }
    else {
      return recorderKey;
    }
  }

  @Override
  public void setup(Context ctx)
  {
    tupleRecordingPartFileSize = ctx.attrValue(LogicalPlan.TUPLE_RECORDING_PART_FILE_SIZE, 100 * 1024);
    tupleRecordingPartFileTimeMillis = ctx.attrValue(LogicalPlan.TUPLE_RECORDING_PART_FILE_TIME_MILLIS, 30 * 60 * 60 * 1000);
    daemonAddress = ctx.attrValue(LogicalPlan.DAEMON_ADDRESS, null);

    RequestDelegateImpl impl = new RequestDelegateImpl();
    RequestFactory rf = RequestFactory.getInstance();
    rf.registerDelegate(RequestType.START_RECORDING, impl);
    rf.registerDelegate(RequestType.STOP_RECORDING, impl);
    rf.registerDelegate(RequestType.SYNC_RECORDING, impl);
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
    return String.valueOf(operatorId).concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(portname);
  }

  private void startRecording(Node<?> node, int operatorId, String portName)
  {
    PortMappingDescriptor descriptor = node.getPortMappingDescriptor();
    String operatorPortName = getRecorderKey(operatorId, portName);
    // check any recording conflict
    boolean conflict = false;
    if (containsKey(String.valueOf(operatorId))) {
      conflict = true;
    }
    else if (portName == null) {
      for (Map.Entry<String, PortContextPair<InputPort<?>>> entry : descriptor.inputPorts.entrySet()) {
        if (containsKey(getRecorderKey(operatorId, entry.getKey()))) {
          conflict = true;
          break;
        }
      }
      for (Map.Entry<String, PortContextPair<OutputPort<?>>> entry : descriptor.outputPorts.entrySet()) {
        if (containsKey(getRecorderKey(operatorId, entry.getKey()))) {
          conflict = true;
          break;
        }
      }
    }
    else {
      if (containsKey(operatorPortName)) {
        conflict = true;
      }
    }
    if (!conflict) {
      logger.debug("Executing start recording request for " + operatorPortName);

      TupleRecorder tupleRecorder = new TupleRecorder();
      String basePath = appPath + "/recordings/" + operatorId + "/" + tupleRecorder.getStartTime();
      tupleRecorder.getStorage().setBasePath(basePath);
      tupleRecorder.getStorage().setBytesPerPartFile(tupleRecordingPartFileSize);
      tupleRecorder.getStorage().setMillisPerPartFile(tupleRecordingPartFileTimeMillis);
      if (daemonAddress != null) {
        String url = "ws://" + daemonAddress + "/pubsub";
        try {
          tupleRecorder.setPubSubUrl(url);
        }
        catch (URISyntaxException ex) {
          logger.warn("URL {} is not valid. NOT posting live tuples to daemon.", url, ex);
        }
      }
      HashMap<String, Sink<Object>> sinkMap = new HashMap<String, Sink<Object>>();
      for (Map.Entry<String, PortContextPair<InputPort<?>>> entry : descriptor.inputPorts.entrySet()) {
        String streamId = getDeclaredStreamId(operatorId, entry.getKey());
        if (streamId == null) {
          streamId = portName + "_implicit_stream";
        }
        if (portName == null || entry.getKey().equals(portName)) {
          logger.info("Adding recorder sink to input port {}, stream {}", entry.getKey(), streamId);
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
          logger.info("Adding recorder sink to output port {}, stream {}", entry.getKey(), streamId);
          tupleRecorder.addOutputPortInfo(entry.getKey(), streamId);
          sinkMap.put(entry.getKey(), tupleRecorder.newSink(entry.getKey()));
        }
      }
      if (!sinkMap.isEmpty()) {
        logger.debug("Started recording (name: {}) to base path {}", operatorPortName, basePath);
        node.addSinks(sinkMap);
        tupleRecorder.setup(node.getOperator());
        put(operatorPortName, tupleRecorder);
      }
      else {
        logger.warn("Tuple recording request ignored because operator is not connected on the specified port.");
      }
    }
    else {
      logger.error("Operator id {} is already being recorded.", operatorPortName);
    }
  }

  private void stopRecording(Node<?> node, int operatorId, String portName)
  {
    String operatorPortName = getRecorderKey(operatorId, portName);
    if (containsKey(operatorPortName)) {
      logger.debug("Executing stop recording request for {}", operatorPortName);

      TupleRecorder tupleRecorder = get(operatorPortName);
      if (tupleRecorder != null) {
        node.removeSinks(tupleRecorder.getSinkMap());
        tupleRecorder.teardown();
        logger.debug("Stopped recording for operator/port {}", operatorPortName);
        remove(operatorPortName);
      }
    }
    // this should be looked at again when we redesign how we handle recordings with ports in a cleaner way
    else if (portName == null) {
      Iterator<Map.Entry<String, TupleRecorder>> iterator = entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, TupleRecorder> entry = iterator.next();
        if (String.valueOf(operatorId).equals(getOperatorFromRecorderKey(entry.getKey()))) {
          TupleRecorder tupleRecorder = entry.getValue();
          if (tupleRecorder != null) {
            node.removeSinks(tupleRecorder.getSinkMap());
            tupleRecorder.teardown();
            logger.debug("Stopped recording for operator/port {}", operatorPortName);
            iterator.remove();
          }
        }
      }
    }
    else {
      logger.error("Operator/port {} is not being recorded.", operatorPortName);
    }
  }

  private void syncRecording(Node<?> node, int operatorId, String portName)
  {
    String operatorPortName = getRecorderKey(operatorId, portName);
    if (containsKey(operatorPortName)) {
      logger.debug("Executing sync recording request for {}" + operatorPortName);

      TupleRecorder tupleRecorder = get(operatorPortName);
      if (tupleRecorder != null) {
        tupleRecorder.getStorage().requestSync();
        logger.debug("Requested sync recording for operator/port {}" + operatorPortName);
      }
    }
    // this should be looked at again when we redesign how we handle recordings with ports in a cleaner way
    else if (portName == null) {
      Iterator<Map.Entry<String, TupleRecorder>> iterator = entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, TupleRecorder> entry = iterator.next();
        if (String.valueOf(operatorId).equals(getOperatorFromRecorderKey(entry.getKey()))) {
          TupleRecorder tupleRecorder = entry.getValue();
          if (tupleRecorder != null) {
            tupleRecorder.getStorage().requestSync();
            logger.debug("Requested sync recording for operator/port {}" + operatorPortName);
          }
        }
      }
    }
    else {
      logger.error("(SYNC_RECORDING) Operator/port " + operatorPortName + " is not being recorded.");
    }
  }

  @Override
  public void activated(Node<?> node)
  {
    for (Map.Entry<String, PortContextPair<InputPort<?>>> entry : node.getPortMappingDescriptor().inputPorts.entrySet()) {
      if (entry.getValue().context.attrValue(PortContext.AUTO_RECORD, false)) {
        startRecording(node, node.getId(), entry.getKey());
      }
    }

    for (Map.Entry<String, PortContextPair<OutputPort<?>>> entry : node.getPortMappingDescriptor().outputPorts.entrySet()) {
      if (entry.getValue().context.attrValue(PortContext.AUTO_RECORD, false)) {
        startRecording(node, node.getId(), entry.getKey());
      }
    }
  }

  @Override
  public void deactivated(Node<?> node)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

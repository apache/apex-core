/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.codec;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context;
import com.malhartech.stram.cli.StramAppLauncher;
import com.malhartech.stram.cli.StramAppLauncher.AppConfig;
import com.malhartech.stram.plan.logical.LogicalPlan;
import com.malhartech.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.malhartech.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.malhartech.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.malhartech.stram.plan.logical.LogicalPlan.StreamMeta;
import java.io.IOException;
import java.util.*;
import javax.ws.rs.Produces;
import javax.ws.rs.ext.Provider;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;


/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
@Provider
@Produces("application/json")
public class AppConfigSerializer extends JsonSerializer<AppConfig>
{

  public static Map<String, Object> convertToMap(AppConfig appConfig)
  {
    HashMap<String, Object> result = new HashMap<String, Object>();
    HashMap<String, Object> operatorMap = new HashMap<String, Object>();
    HashMap<String, Object> streamMap = new HashMap<String, Object>();
    result.put("applicationName", appConfig.getName());
    result.put("operators", operatorMap);
    result.put("streams", streamMap);
    LogicalPlan dag = StramAppLauncher.prepareDAG(appConfig, ApplicationFactory.LAUNCHMODE_YARN);

    //
    // should we put the DAGContext info here?
    //

    Collection<OperatorMeta> allOperators = dag.getAllOperators();

    for (OperatorMeta operatorMeta : allOperators) {
      HashMap<String, Object> operatorDetailMap = new HashMap<String, Object>();
      HashMap<String, Object> portMap = new HashMap<String, Object>();
      String operatorName = operatorMeta.getId();
      operatorMap.put(operatorName, operatorDetailMap);
      operatorDetailMap.put("ports", portMap);
      operatorDetailMap.put("class", operatorMeta.getOperator().getClass().getName());
      for (Context.OperatorContext.AttributeKey<?> attrKey
              : new Context.OperatorContext.AttributeKey<?>[] {
        Context.OperatorContext.APPLICATION_WINDOW_COUNT,
        Context.OperatorContext.INITIAL_PARTITION_COUNT,
        Context.OperatorContext.MEMORY_MB,
        Context.OperatorContext.PARTITION_TPS_MAX,
        Context.OperatorContext.PARTITION_TPS_MIN,
        Context.OperatorContext.RECOVERY_ATTEMPTS,
        Context.OperatorContext.SPIN_MILLIS,
        Context.OperatorContext.STATELESS}) {
        Object attrValue = operatorMeta.getAttributes().attrValue(attrKey, null);
        if (attrValue != null) {
          operatorDetailMap.put(attrKey.name(), attrValue);
        }
      }

      Map<InputPortMeta, StreamMeta> inputStreams = operatorMeta.getInputStreams();
      for (Map.Entry<InputPortMeta, StreamMeta> entry : inputStreams.entrySet()) {
        HashMap<String, Object> portDetailMap = new HashMap<String, Object>();
        InputPortMeta portMeta = entry.getKey();
        String portName = portMeta.getPortName();
        portDetailMap.put("type", "input");
        for (Context.PortContext.AttributeKey<?> attrKey
                : new Context.PortContext.AttributeKey<?>[] {
          Context.PortContext.QUEUE_CAPACITY,
          Context.PortContext.PARTITION_PARALLEL,
          Context.PortContext.SPIN_MILLIS}) {
          Object attrValue = portMeta.getAttributes().attrValue(attrKey, null);
          if (attrValue != null) {
            operatorDetailMap.put(attrKey.name(), attrValue);
          }
        }
        portMap.put(portName, portDetailMap);
      }
      Map<OutputPortMeta, StreamMeta> outputStreams = operatorMeta.getOutputStreams();
      for (Map.Entry<OutputPortMeta, StreamMeta> entry : outputStreams.entrySet()) {
        HashMap<String, Object> portDetailMap = new HashMap<String, Object>();
        OutputPortMeta portMeta = entry.getKey();
        String portName = portMeta.getPortName();
        portDetailMap.put("type", "output");
        for (Context.PortContext.AttributeKey<?> attrKey
                : new Context.PortContext.AttributeKey<?>[] {
          Context.PortContext.QUEUE_CAPACITY,
          Context.PortContext.PARTITION_PARALLEL,
          Context.PortContext.SPIN_MILLIS}) {
          Object attrValue = portMeta.getAttributes().attrValue(attrKey, null);
          if (attrValue != null) {
            operatorDetailMap.put(attrKey.name(), attrValue);
          }
        }
        portMap.put(portName, portDetailMap);
      }
    }
    Collection<StreamMeta> allStreams = dag.getAllStreams();

    for (StreamMeta streamMeta : allStreams) {
      HashMap<String, Object> streamDetailMap = new HashMap<String, Object>();
      String streamName = streamMeta.getId();
      streamMap.put(streamName, streamDetailMap);
      String sourcePortName = streamMeta.getSource().getPortName();
      OperatorMeta operatorMeta = streamMeta.getSource().getOperatorWrapper();
      HashMap<String, Object> sourcePortDetailMap = new HashMap<String, Object>();
      sourcePortDetailMap.put("operatorName", operatorMeta.getId());
      sourcePortDetailMap.put("portName", sourcePortName);
      streamDetailMap.put("source", sourcePortDetailMap);
      List<InputPortMeta> sinks = streamMeta.getSinks();
      ArrayList<HashMap<String, Object>> sinkPortList = new ArrayList<HashMap<String, Object>>();
      for (InputPortMeta sinkPort : sinks) {
        HashMap<String, Object> sinkPortDetailMap = new HashMap<String, Object>();
        sinkPortDetailMap.put("operatorName", sinkPort.getOperatorWrapper().getId());
        sinkPortDetailMap.put("portName", sinkPort.getPortName());
        sinkPortList.add(sinkPortDetailMap);
      }
      streamDetailMap.put("sinks", sinkPortList);
      streamDetailMap.put("isInline", streamMeta.isInline());
    }
    return result;
  }

  @Override
  public void serialize(AppConfig appConfig, JsonGenerator jg, SerializerProvider sp) throws IOException, JsonProcessingException
  {
    jg.writeObject(convertToMap(appConfig));
  }
}

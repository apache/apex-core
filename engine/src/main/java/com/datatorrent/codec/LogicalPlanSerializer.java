/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.codec;

import java.io.IOException;
import java.util.*;

import javax.ws.rs.Produces;
import javax.ws.rs.ext.Provider;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import com.datatorrent.stram.DAGPropertiesBuilder;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
@Provider
@Produces("application/json")
public class LogicalPlanSerializer extends JsonSerializer<LogicalPlan>
{
  /**
   *
   * @param dag
   * @return
   */
  public static Map<String, Object> convertToMap(LogicalPlan dag)
  {
    HashMap<String, Object> result = new HashMap<String, Object>();
    HashMap<String, Object> operatorMap = new HashMap<String, Object>();
    HashMap<String, Object> streamMap = new HashMap<String, Object>();
    //result.put("applicationName", appConfig.getName());
    result.put("operators", operatorMap);
    result.put("streams", streamMap);
    //LogicalPlan dag = StramAppLauncher.prepareDAG(appConfig, StreamingApplication.LAUNCHMODE_YARN);
    //
    // should we put the DAGContext info here?
    //

    Collection<OperatorMeta> allOperators = dag.getAllOperators();

    for (OperatorMeta operatorMeta : allOperators) {
      HashMap<String, Object> operatorDetailMap = new HashMap<String, Object>();
      HashMap<String, Object> portMap = new HashMap<String, Object>();
      HashMap<String, Object> attributeMap = new HashMap<String, Object>();
      HashMap<String, Object> propertyMap = new HashMap<String, Object>();

      String operatorName = operatorMeta.getName();
      operatorMap.put(operatorName, operatorDetailMap);
      operatorDetailMap.put("ports", portMap);
      operatorDetailMap.put("class", operatorMeta.getOperator().getClass().getName());
      operatorDetailMap.put("attributes", attributeMap);
      operatorDetailMap.put("properties", propertyMap);
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
        Object attrValue = operatorMeta.getAttributes().attr(attrKey).get();
        if (attrValue != null) {
          attributeMap.put(attrKey.name(), attrValue);
        }
      }
      Map<String, Object> operatorProperties = DAGPropertiesBuilder.getOperatorProperties(operatorMeta.getOperator());
      for (Map.Entry<String, Object> entry : operatorProperties.entrySet()) {
        if (entry.getValue() != null) {
          propertyMap.put(entry.getKey(), entry.getValue());
        }
      }

      Map<InputPortMeta, StreamMeta> inputStreams = operatorMeta.getInputStreams();
      for (Map.Entry<InputPortMeta, StreamMeta> entry : inputStreams.entrySet()) {
        HashMap<String, Object> portDetailMap = new HashMap<String, Object>();
        HashMap<String, Object> portAttributeMap = new HashMap<String, Object>();
        InputPortMeta portMeta = entry.getKey();
        String portName = portMeta.getPortName();
        portDetailMap.put("type", "input");
        portDetailMap.put("attributes", portAttributeMap);
        for (Context.PortContext.AttributeKey<?> attrKey
                : new Context.PortContext.AttributeKey<?>[] {
          Context.PortContext.QUEUE_CAPACITY,
          Context.PortContext.PARTITION_PARALLEL,
          Context.PortContext.SPIN_MILLIS}) {
          Object attrValue = portMeta.getAttributes().attr(attrKey).get();
          if (attrValue != null) {
            portAttributeMap.put(attrKey.name(), attrValue);
          }
        }
        portMap.put(portName, portDetailMap);
      }
      Map<OutputPortMeta, StreamMeta> outputStreams = operatorMeta.getOutputStreams();
      for (Map.Entry<OutputPortMeta, StreamMeta> entry : outputStreams.entrySet()) {
        HashMap<String, Object> portDetailMap = new HashMap<String, Object>();
        HashMap<String, Object> portAttributeMap = new HashMap<String, Object>();
        OutputPortMeta portMeta = entry.getKey();
        String portName = portMeta.getPortName();
        portDetailMap.put("type", "output");
        portDetailMap.put("attributes", portAttributeMap);
        for (Context.PortContext.AttributeKey<?> attrKey
                : new Context.PortContext.AttributeKey<?>[] {
          Context.PortContext.QUEUE_CAPACITY,
          Context.PortContext.PARTITION_PARALLEL,
          Context.PortContext.SPIN_MILLIS}) {
          Object attrValue = portMeta.getAttributes().attr(attrKey).get();
          if (attrValue != null) {
            portAttributeMap.put(attrKey.name(), attrValue);
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
      sourcePortDetailMap.put("operatorName", operatorMeta.getName());
      sourcePortDetailMap.put("portName", sourcePortName);
      streamDetailMap.put("source", sourcePortDetailMap);
      List<InputPortMeta> sinks = streamMeta.getSinks();
      ArrayList<HashMap<String, Object>> sinkPortList = new ArrayList<HashMap<String, Object>>();
      for (InputPortMeta sinkPort : sinks) {
        HashMap<String, Object> sinkPortDetailMap = new HashMap<String, Object>();
        sinkPortDetailMap.put("operatorName", sinkPort.getOperatorWrapper().getName());
        sinkPortDetailMap.put("portName", sinkPort.getPortName());
        sinkPortList.add(sinkPortDetailMap);
      }
      streamDetailMap.put("sinks", sinkPortList);
      streamDetailMap.put("isInline", streamMeta.isInline());
    }
    return result;
  }

  public static PropertiesConfiguration convertToProperties(LogicalPlan dag)
  {
    PropertiesConfiguration props = new PropertiesConfiguration();
    Collection<OperatorMeta> allOperators = dag.getAllOperators();

    for (OperatorMeta operatorMeta : allOperators) {
      String operatorKey = "stram.operator." + operatorMeta.getName();
      Operator operator = operatorMeta.getOperator();
      props.setProperty(operatorKey + ".classname", operator.getClass().getName());
      Map<String, Object> operatorProperties = DAGPropertiesBuilder.getOperatorProperties(operator);
      for (Map.Entry<String, Object> entry : operatorProperties.entrySet()) {
        if (!entry.getKey().equals("class") && !entry.getKey().equals("name") && entry.getValue() != null) {
          props.setProperty(operatorKey + "." + entry.getKey(), entry.getValue());
        }
      }
    }
    Collection<StreamMeta> allStreams = dag.getAllStreams();

    for (StreamMeta streamMeta : allStreams) {
      String streamKey = "stram.stream." + streamMeta.getId();
      OutputPortMeta source = streamMeta.getSource();
      List<InputPortMeta> sinks = streamMeta.getSinks();
      props.setProperty(streamKey + ".source", source.getOperatorWrapper().getName() + "." + source.getPortName());
      String sinksValue = "";
      for (InputPortMeta sink : sinks) {
        if (!sinksValue.isEmpty()) {
          sinksValue += ",";
        }
        sinksValue += sink.getOperatorWrapper().getName() + "." + sink.getPortName();
      }
      props.setProperty(streamKey + ".sinks", sinksValue);
      if (streamMeta.isInline()) {
        props.setProperty(streamKey + ".inline", "true");
      }
    }

    // TBD: Attributes

    return props;
  }

  public static PropertiesConfiguration convertToProperties(JSONObject json) throws JSONException
  {
    PropertiesConfiguration props = new PropertiesConfiguration();
    JSONObject allOperators = json.getJSONObject("operators");
    JSONObject allStreams = json.getJSONObject("streams");

    @SuppressWarnings("unchecked")
    Iterator<String> operatorIter = allOperators.keys();
    while (operatorIter.hasNext()) {
      String operatorName = operatorIter.next();
      JSONObject operatorDetail = allOperators.getJSONObject(operatorName);
      String operatorKey = "stram.operator." + operatorName;
      props.setProperty(operatorKey + ".classname", operatorDetail.getString("class"));
      JSONObject properties = operatorDetail.optJSONObject("properties");
      if (properties != null) {
        @SuppressWarnings("unchecked")
        Iterator<String> iter2 = properties.keys();
        while (iter2.hasNext()) {
          String propertyName = iter2.next();
          if (!propertyName.equals("name") && !propertyName.equals("class") && properties.opt(propertyName) != null) {
            JSONArray list = properties.optJSONArray(propertyName);
            String value = "";
            if (list != null) {
              for (int i = 0; i < list.length(); i++) {
                if (i != 0) {
                  value += ",";
                }
                value += list.get(i).toString();
              }
              props.setProperty(operatorKey + "." + propertyName, value);
            } else {
              props.setProperty(operatorKey + "." + propertyName, properties.get(propertyName));
            }
          }
        }
      }
    }

    @SuppressWarnings("unchecked")
    Iterator<String> streamIter = allStreams.keys();
    while (streamIter.hasNext()) {
      String streamName = streamIter.next();
      JSONObject streamDetail = allStreams.getJSONObject(streamName);
      String streamKey = "stram.stream." + streamName;
      JSONObject sourceDetail = streamDetail.getJSONObject("source");
      JSONArray sinksList = streamDetail.getJSONArray("sinks");

      props.setProperty(streamKey + ".source", sourceDetail.getString("operatorName") + "." + sourceDetail.getString("portName"));
      String sinksValue = "";
      for (int i = 0; i < sinksList.length(); i++) {
        if (!sinksValue.isEmpty()) {
          sinksValue += ",";
        }
        sinksValue += sinksList.getJSONObject(i).getString("operatorName") + "." + sinksList.getJSONObject(i).getString("portName");
      }
      props.setProperty(streamKey + ".sinks", sinksValue);
      if (streamDetail.optBoolean("isInline")) {
        props.setProperty(streamKey + ".inline", "true");
      }
    }

    // TBD: Attributes

    return props;
  }

  @Override
  public void serialize(LogicalPlan dag, JsonGenerator jg, SerializerProvider sp) throws IOException, JsonProcessingException
  {
    jg.writeObject(convertToMap(dag));
  }

}

/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.codec;

import com.datatorrent.api.*;
import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.lib.util.ObjectMapperString;
import com.datatorrent.stram.plan.logical.*;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;
import java.io.IOException;
import java.util.*;
import javax.ws.rs.Produces;
import javax.ws.rs.ext.Provider;
import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.map.ObjectMapper.DefaultTypeResolverBuilder;
import org.codehaus.jackson.map.ObjectMapper.DefaultTyping;
import org.codehaus.jackson.map.jsontype.impl.StdTypeResolverBuilder;
import org.codehaus.jackson.type.JavaType;
import org.codehaus.jettison.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>LogicalPlanSerializer class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
@Provider
@Produces("application/json")
public class LogicalPlanSerializer extends JsonSerializer<LogicalPlan>
{
  private static class PropertyTypeResolverBuilder extends DefaultTypeResolverBuilder
  {
    PropertyTypeResolverBuilder()
    {
      super(DefaultTyping.NON_FINAL);
    }

    @Override
    public boolean useForType(JavaType t)
    {
      if (t.getRawClass() == Object.class) {
        return true;
      }
      if (t.getRawClass().getName().startsWith("java.")) {
        return false;
      }
      if (t.isArrayType()) {
        return false;
      }
      return super.useForType(t);
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlanSerializer.class);

  /**
   *
   * @param dag
   * @return
   */
  public static Map<String, Object> convertToMap(LogicalPlan dag)
  {
    HashMap<String, Object> result = new HashMap<String, Object>();
    ArrayList<Object> operatorArray = new ArrayList< Object>();
    ArrayList<Object> streamMap = new ArrayList<Object>();
    //result.put("applicationName", appConfig.getName());
    result.put("operators", operatorArray);
    result.put("streams", streamMap);
    //LogicalPlan dag = StramAppLauncher.prepareDAG(appConfig, StreamingApplication.LAUNCHMODE_YARN);
    //
    // should we put the DAGContext info here?
    //

    Collection<OperatorMeta> allOperators = dag.getAllOperators();
    ObjectMapper propertyObjectMapper = new ObjectMapper();
    propertyObjectMapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
    propertyObjectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);

    StdTypeResolverBuilder typer = new PropertyTypeResolverBuilder();
    typer.init(JsonTypeInfo.Id.CLASS, null);
    typer = typer.inclusion(JsonTypeInfo.As.PROPERTY);
    propertyObjectMapper.setDefaultTyping(typer);

    for (OperatorMeta operatorMeta : allOperators) {
      HashMap<String, Object> operatorDetailMap = new HashMap<String, Object>();
      ArrayList<Map<String, Object>> portList = new ArrayList<Map<String, Object>>();
      Map<String, Object> attributeMap = new HashMap<String, Object>();

      String operatorName = operatorMeta.getName();
      operatorArray.add(operatorDetailMap);
      operatorDetailMap.put("name", operatorName);
      operatorDetailMap.put("ports", portList);
      operatorDetailMap.put("class", operatorMeta.getOperator().getClass().getName());
      operatorDetailMap.put("attributes", attributeMap);
      Map<Attribute<Object>, Object> rawAttributes = AttributeMap.AttributeInitializer.getAllAttributes(operatorMeta, Context.OperatorContext.class);
      for (Map.Entry<Attribute<Object>, Object> entry : rawAttributes.entrySet()) {
        attributeMap.put(entry.getKey().getSimpleName(), entry.getValue());
      }

      ObjectMapperString str;

      try {
        str = new ObjectMapperString(propertyObjectMapper.writeValueAsString(operatorMeta.getOperator()));
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      operatorDetailMap.put("properties", str);

      Operators.PortMappingDescriptor pmd = new Operators.PortMappingDescriptor();
      Operators.describe(operatorMeta.getOperator(), pmd);
      for (Map.Entry<String, PortContextPair<InputPort<?>>> entry : pmd.inputPorts.entrySet()) {
        HashMap<String, Object> portDetailMap = new HashMap<String, Object>();
        HashMap<String, Object> portAttributeMap = new HashMap<String, Object>();
        InputPortMeta portMeta = operatorMeta.getMeta(entry.getValue().component);
        String portName = portMeta.getPortName();
        portDetailMap.put("name", portName);
        portDetailMap.put("type", "input");
        portDetailMap.put("tupleType", portMeta.getTupleTypeString());
        portDetailMap.put("attributes", portAttributeMap);
        rawAttributes = AttributeMap.AttributeInitializer.getAllAttributes(portMeta, Context.PortContext.class);
        for (Map.Entry<Attribute<Object>, Object> attEntry : rawAttributes.entrySet()) {
          portAttributeMap.put(attEntry.getKey().getSimpleName(), attEntry.getValue());
        }
        portList.add(portDetailMap);
      }
      for (Map.Entry<String, PortContextPair<OutputPort<?>>> entry : pmd.outputPorts.entrySet()) {
        HashMap<String, Object> portDetailMap = new HashMap<String, Object>();
        HashMap<String, Object> portAttributeMap = new HashMap<String, Object>();
        OutputPortMeta portMeta = operatorMeta.getMeta(entry.getValue().component);
        String portName = portMeta.getPortName();
        portDetailMap.put("name", portName);
        portDetailMap.put("type", "output");
        portDetailMap.put("tupleType", portMeta.getTupleTypeString());
        portDetailMap.put("attributes", portAttributeMap);
        rawAttributes = AttributeMap.AttributeInitializer.getAllAttributes(portMeta, Context.PortContext.class);
        for (Map.Entry<Attribute<Object>, Object> attEntry : rawAttributes.entrySet()) {
          portAttributeMap.put(attEntry.getKey().getSimpleName(), attEntry.getValue());
        }
        portList.add(portDetailMap);
      }
    }
    Collection<StreamMeta> allStreams = dag.getAllStreams();

    for (StreamMeta streamMeta : allStreams) {
      HashMap<String, Object> streamDetailMap = new HashMap<String, Object>();
      String streamName = streamMeta.getName();
      streamMap.add(streamDetailMap);
      String sourcePortName = streamMeta.getSource().getPortName();
      OperatorMeta operatorMeta = streamMeta.getSource().getOperatorWrapper();
      HashMap<String, Object> sourcePortDetailMap = new HashMap<String, Object>();
      sourcePortDetailMap.put("operatorName", operatorMeta.getName());
      sourcePortDetailMap.put("portName", sourcePortName);
      streamDetailMap.put("name", streamName);
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
      if (streamMeta.getLocality() != null) {
        streamDetailMap.put("locality", streamMeta.getLocality().name());
      }
    }
    return result;
  }

  public static PropertiesConfiguration convertToProperties(LogicalPlan dag)
  {
    PropertiesConfiguration props = new PropertiesConfiguration();
    Collection<OperatorMeta> allOperators = dag.getAllOperators();

    for (OperatorMeta operatorMeta : allOperators) {
      String operatorKey = LogicalPlanConfiguration.OPERATOR_PREFIX + operatorMeta.getName();
      Operator operator = operatorMeta.getOperator();
      props.setProperty(operatorKey + "." + LogicalPlanConfiguration.OPERATOR_CLASSNAME, operator.getClass().getName());
      BeanMap operatorProperties = LogicalPlanConfiguration.getOperatorProperties(operator);
      @SuppressWarnings("rawtypes")
      Iterator entryIterator = operatorProperties.entryIterator();
      while (entryIterator.hasNext()) {
        try {
          @SuppressWarnings("unchecked")
          Map.Entry<String, Object> entry = (Map.Entry<String, Object>)entryIterator.next();
          if (!entry.getKey().equals("class") && !entry.getKey().equals("name") && entry.getValue() != null) {
            props.setProperty(operatorKey + "." + entry.getKey(), entry.getValue());
          }
        }
        catch (Exception ex) {
          LOG.warn("Error trying to get a property of operator {}", operatorMeta.getName(), ex);
        }
      }
    }
    Collection<StreamMeta> allStreams = dag.getAllStreams();

    for (StreamMeta streamMeta : allStreams) {
      String streamKey = LogicalPlanConfiguration.STREAM_PREFIX + streamMeta.getName();
      OutputPortMeta source = streamMeta.getSource();
      List<InputPortMeta> sinks = streamMeta.getSinks();
      props.setProperty(streamKey + "." + LogicalPlanConfiguration.STREAM_SOURCE, source.getOperatorWrapper().getName() + "." + source.getPortName());
      String sinksValue = "";
      for (InputPortMeta sink : sinks) {
        if (!sinksValue.isEmpty()) {
          sinksValue += ",";
        }
        sinksValue += sink.getOperatorWrapper().getName() + "." + sink.getPortName();
      }
      props.setProperty(streamKey + "." + LogicalPlanConfiguration.STREAM_SINKS, sinksValue);
      if (streamMeta.getLocality() != null) {
        props.setProperty(streamKey + "." + LogicalPlanConfiguration.STREAM_LOCALITY, streamMeta.getLocality().name());
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
      String operatorKey = LogicalPlanConfiguration.OPERATOR_PREFIX + operatorName;
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
            }
            else {
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
      String streamKey = LogicalPlanConfiguration.STREAM_PREFIX + streamName;
      JSONObject sourceDetail = streamDetail.getJSONObject("source");
      JSONArray sinksList = streamDetail.getJSONArray("sinks");

      props.setProperty(streamKey + "." + LogicalPlanConfiguration.STREAM_SOURCE, sourceDetail.getString("operatorName") + "." + sourceDetail.getString("portName"));
      String sinksValue = "";
      for (int i = 0; i < sinksList.length(); i++) {
        if (!sinksValue.isEmpty()) {
          sinksValue += ",";
        }
        sinksValue += sinksList.getJSONObject(i).getString("operatorName") + "." + sinksList.getJSONObject(i).getString("portName");
      }
      props.setProperty(streamKey + "." + LogicalPlanConfiguration.STREAM_SINKS, sinksValue);
      String locality = streamDetail.optString("locality");
      if (locality != null) {
        props.setProperty(streamKey + "." + LogicalPlanConfiguration.STREAM_LOCALITY, Locality.valueOf(locality));
      }
    }

    // TBD: Attributes

    return props;
  }

  public static JSONObject convertToJsonObject(LogicalPlan dag)
  {
    return new JSONObject(convertToMap(dag));
  }

  @Override
  public void serialize(LogicalPlan dag, JsonGenerator jg, SerializerProvider sp) throws IOException, JsonProcessingException
  {
    jg.writeObject(convertToMap(dag));
  }

}

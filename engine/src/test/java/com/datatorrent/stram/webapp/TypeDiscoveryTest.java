/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

public class TypeDiscoveryTest
{

  private static interface GenericInterface<T> {
  }

  private static class StringOutputPort extends DefaultOutputPort<String> {
    public StringOutputPort(Operator operator) {
      super();
    }
  }

  static class ParameterizedOperator<T0, T1 extends Map<String, ? extends T0>, T2 extends Number> extends BaseOperator implements GenericInterface<T1> {
    final InputPort<T1> inputT1 = new DefaultInputPort<T1>() {
      @Override
      public void process(T1 tuple) {
      }
    };
    final OutputPort<T2> outportT2 = new DefaultOutputPort<T2>();
    final OutputPort<Number> outportNumberParam = new DefaultOutputPort<Number>();
    final StringOutputPort outportString = new StringOutputPort(this);
    final OutputPort<List<T0>> outportList = new DefaultOutputPort<List<T0>>();
  }

  static class ExtendsParameterizedOperator extends ParameterizedOperator<Number, Map<String, Double>, Double>
  {
  }

  static class SpecializedOperator extends BaseOperator {
    final InputPort<String> inputT1 = new DefaultInputPort<String>() {
      @Override
      public void process(String tuple) {
      }
    };
    final OutputPort<Map<String, Number>> outportT2 = new DefaultOutputPort<Map<String, Number>>();
    final OutputPort<Number> outportNumberParam = new DefaultOutputPort<Number>();
    final StringOutputPort outportString = new StringOutputPort(this);
  }

  public static class DiscoveryContext
  {
    JSONObject json = new JSONObject();
    Map<String, String> resolvedTypeParams = Maps.newHashMap();
  }

  @Test
  public void testTypeDiscovery() throws Exception
  {
    TypeDiscoverer td = new TypeDiscoverer();
    JSONArray json = td.getPortTypes(ExtendsParameterizedOperator.class);
    System.out.println(json.toString(2));
    System.out.println(td.typeArguments);

    ObjectMapper mapper = new ObjectMapper();
    //List<?> l = mapper.convertValue(json, List.class);
    JsonNode root = mapper.readTree(json.toString(2));
    String val = root.get(0).path("name").asText();
    Assert.assertEquals("port name", "inputT1", val);

    val = root.get(4).path("name").asText();
    Assert.assertEquals("port name", "outportList", val);

    val = root.get(4).path("type").asText();
    Assert.assertEquals("outportList type", "java.util.List", val);

    val = root.get(4).path("typeArgs").get(0).path("type").asText();
    Assert.assertEquals("outportList type", "java.lang.Number", val);
  }

  static class ParameterizedTypeOperator<T> extends BaseOperator
  {
    public transient final OutputPort<T> output = new DefaultOutputPort<T>();
  }

  static class StringParameterOperator extends ParameterizedTypeOperator<String>
  {
  }

  @Test
  public void testTypeDiscovery2() throws Exception
  {
    TypeDiscoverer td = new TypeDiscoverer();
    JSONArray json = td.getPortTypes(StringParameterOperator.class);
    //System.out.println(json.toString(2));
    //System.out.println(td.typeArguments);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(json.toString(2));
    String val = root.get(0).path("name").asText();
    Assert.assertEquals("port name", "output", val);

    val = root.get(0).path("type").asText();
    Assert.assertEquals("port type", "java.lang.String", val);
  }


  static class SubClass<T> extends ParameterizedTypeOperator<T>
  {
  }

  static class SubSubClass extends SubClass<Map<String, Object>>
  {
  }

  @Test
  public void testTypeDiscoveryMultiLevel() throws Exception
  {
    TypeDiscoverer td = new TypeDiscoverer();
    JSONArray json = td.getPortTypes(SubSubClass.class);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(json.toString(2));
    String val = root.get(0).path("name").asText();
    Assert.assertEquals("port name", "output", val);

    val = root.get(0).path("type").asText();
    Assert.assertEquals("port type", "java.util.Map", val);

    val = root.get(0).path("typeArgs").get(0).path("type").asText();
    Assert.assertEquals("map key type", "java.lang.String", val);
    val = root.get(0).path("typeArgs").get(1).path("type").asText();
    Assert.assertEquals("map value type", "java.lang.Object", val);

  }

  @Test
  public void testAppAttributes() throws JSONException, IllegalAccessException
  {
    JSONArray appAttributes = TypeDiscoverer.getAppAttributes().getJSONArray("attributes");
    Map<String, JSONObject> attributesMap = Maps.newHashMap();
    for (int i = 0; i < appAttributes.length(); i++) {
      attributesMap.put(appAttributes.getJSONObject(i).getString("name"), appAttributes.getJSONObject(i));
    }
    JSONObject appNameAttr = attributesMap.get("APPLICATION_NAME");
    Assert.assertNotNull("application name", appNameAttr);
    Assert.assertEquals("application name type", "java.lang.String", appNameAttr.getString("type"));
    Assert.assertNotNull("default app name", appNameAttr.getString("default"));

    JSONObject stringCodecsAttr = attributesMap.get("STRING_CODECS");
    Assert.assertNotNull("string codecs", stringCodecsAttr);
    Assert.assertEquals("string codecs type", "java.util.Map", stringCodecsAttr.getString("type"));
    Assert.assertNotNull("type args", stringCodecsAttr.getJSONArray("typeArgs"));
  }

  @Test
  public void testOperatorAttributes() throws JSONException, IllegalAccessException
  {
    JSONArray operatorAttributes = TypeDiscoverer.getOperatorAttributes().getJSONArray("attributes");
    Map<String, JSONObject> attributesMap = Maps.newHashMap();
    for (int i = 0; i < operatorAttributes.length(); i++) {
      attributesMap.put(operatorAttributes.getJSONObject(i).getString("name"), operatorAttributes.getJSONObject(i));
    }
    JSONObject activationWindowAttr = attributesMap.get("ACTIVATION_WINDOW_ID");
    Assert.assertNotNull("activation window", activationWindowAttr);
    Assert.assertEquals("activation window type", "java.lang.Long", activationWindowAttr.getString("type"));
    Assert.assertEquals("default activation window", "-1", activationWindowAttr.getString("default"));

    JSONObject partitionerAttr = attributesMap.get("PARTITIONER");
    Assert.assertNotNull("partitioner", partitionerAttr);
    Assert.assertEquals("partitioner type", "com.datatorrent.api.Partitioner", partitionerAttr.getString("type"));
    Assert.assertNotNull("type args", partitionerAttr.getJSONArray("typeArgs"));
  }

  @Test
  public void testPortAttributes() throws JSONException, IllegalAccessException
  {
    JSONArray portAttributes = TypeDiscoverer.getPortAttributes().getJSONArray("attributes");
    Map<String, JSONObject> attributesMap = Maps.newHashMap();
    for (int i = 0; i < portAttributes.length(); i++) {
      attributesMap.put(portAttributes.getJSONObject(i).getString("name"), portAttributes.getJSONObject(i));
    }
    JSONObject queueCapacityAttr = attributesMap.get("QUEUE_CAPACITY");
    Assert.assertNotNull("queue capacity", queueCapacityAttr);
    Assert.assertEquals("queue capacity type", "java.lang.Integer", queueCapacityAttr.getString("type"));
    Assert.assertEquals("default queue capacity", "1024", queueCapacityAttr.getString("default"));

    JSONObject streamCodecAttr = attributesMap.get("STREAM_CODEC");
    Assert.assertNotNull("stream codec", streamCodecAttr);
    Assert.assertEquals("stream codec type", "com.datatorrent.api.StreamCodec", streamCodecAttr.getString("type"));
    Assert.assertNotNull("type args", streamCodecAttr.getJSONArray("typeArgs"));
  }

}

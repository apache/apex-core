/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

public class TypeDiscoveryTest
{

  private interface GenericInterface<T>
  {
  }

  private static class StringOutputPort extends DefaultOutputPort<String>
  {
    public StringOutputPort(Operator operator)
    {
      super();
    }
  }

  private static class A<K>
  {

  }

  private static class B<T extends Number> extends A
  {

  }

  private static class GenericOutputPort extends DefaultOutputPort<B<Number>>
  {
    public GenericOutputPort(Operator operator)
    {
      super();
    }
  }

  private static class GenericSubClassOutputPort extends GenericOutputPort
  {
    public GenericSubClassOutputPort(Operator operator)
    {
      super(operator);
    }
  }

  static class ParameterizedOperator<T0, T1 extends Map<String, ? extends T0>, T2 extends Number>
      extends BaseOperator implements GenericInterface<T1>
  {
    final InputPort<T1> inputT1 = new DefaultInputPort<T1>()
    {
      @Override
      public void process(T1 tuple)
      {
      }
    };
    final OutputPort<T2> outportT2 = new DefaultOutputPort<>();
    final OutputPort<Number> outportNumberParam = new DefaultOutputPort<>();
    final StringOutputPort outportString = new StringOutputPort(this);
    final OutputPort<List<T0>> outportList = new DefaultOutputPort<>();
    final GenericSubClassOutputPort outClassObject = new GenericSubClassOutputPort(this);
  }

  static class ExtendsParameterizedOperator extends ParameterizedOperator<Number, Map<String, Double>, Double>
  {
  }

  static class SpecializedOperator extends BaseOperator
  {
    final InputPort<String> inputT1 = new DefaultInputPort<String>()
    {
      @Override
      public void process(String tuple)
      {
      }
    };
    final OutputPort<Map<String, Number>> outportT2 = new DefaultOutputPort<>();
    final OutputPort<Number> outportNumberParam = new DefaultOutputPort<>();
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
    String[] classFilePath = OperatorDiscoveryTest.getClassFileInClasspath();
    OperatorDiscoverer od = new OperatorDiscoverer(classFilePath);
    od.buildTypeGraph();

    JSONObject Desc = od.describeClassByASM(ExtendsParameterizedOperator.class.getName());
    JSONArray json = Desc.getJSONArray("portTypeInfo");

    String debug = "\n(ASM)type info for " + ExtendsParameterizedOperator.class + ":\n" + Desc.toString(2) + "\n";

    ObjectMapper mapper = new ObjectMapper();
    //List<?> l = mapper.convertValue(json, List.class);
    JsonNode root = mapper.readTree(json.toString(2));
    String val = root.get(0).path("name").asText();
    Assert.assertEquals(debug + "port name", "inputT1", val);

    val = root.get(3).path("name").asText();
    Assert.assertEquals(debug + "port name", "outportString", val);

    val = root.get(3).path("type").asText();
    Assert.assertEquals(debug + "outportList type", "java.lang.String", val);

    val = root.get(4).path("name").asText();
    Assert.assertEquals(debug + "port name", "outportList", val);

    val = root.get(4).path("type").asText();
    Assert.assertEquals(debug + "outportList type", "java.util.List", val);

    val = root.get(4).path("typeArgs").get(0).path("type").asText();
    Assert.assertEquals(debug + "outportList type", "java.lang.Number", val);

    val = root.get(5).path("name").asText();
    Assert.assertEquals(debug + "port name", "outClassObject", val);

    val = root.get(5).path("type").asText();
    Assert.assertEquals(debug + "outportList type", "com.datatorrent.stram.webapp.TypeDiscoveryTest$B", val);

    val = root.get(5).path("typeArgs").get(0).path("type").asText();
    Assert.assertEquals(debug + "outportList type", "java.lang.Number", val);
  }

  static class ParameterizedTypeOperator<T> extends BaseOperator
  {
    final OutputPort<T> output = new DefaultOutputPort<>();
  }

  static class StringParameterOperator extends ParameterizedTypeOperator<String>
  {
  }

  @Test
  public void testTypeDiscovery2() throws Exception
  {
    String[] classFilePath = OperatorDiscoveryTest.getClassFileInClasspath();
    OperatorDiscoverer od = new OperatorDiscoverer(classFilePath);
    od.buildTypeGraph();

    JSONObject Desc = od.describeClassByASM(StringParameterOperator.class.getName());
    JSONArray json = Desc.getJSONArray("portTypeInfo");
    String debug = "\n(ASM)type info for " + StringParameterOperator.class + ":\n" + Desc.toString(2) + "\n";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(json.toString(2));
    String val = root.get(0).path("name").asText();
    Assert.assertEquals(debug + "port name", "output", val);

    val = root.get(0).path("type").asText();
    Assert.assertEquals(debug + "port type", "java.lang.String", val);
  }

  static class SubClass<K> extends ParameterizedTypeOperator<K>
  {
  }

  static class SubSubClass extends SubClass<Map<String, Object>>
  {
    public String trial;
  }

  @Test
  public void testTypeDiscoveryMultiLevel() throws Exception
  {
    String[] classFilePath = OperatorDiscoveryTest.getClassFileInClasspath();
    OperatorDiscoverer od = new OperatorDiscoverer(classFilePath);
    od.buildTypeGraph();

    JSONObject Desc = od.describeClassByASM(SubSubClass.class.getName());
    JSONArray json = Desc.getJSONArray("portTypeInfo");
    String debug = "\n(ASM)type info for " + SubSubClass.class + ":\n" + Desc.toString(2) + "\n";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(json.toString(2));
    String val = root.get(0).path("name").asText();
    Assert.assertEquals(debug + "port name", "output", val);

    val = root.get(0).path("type").asText();
    Assert.assertEquals(debug + "port type", "java.util.Map", val);

    val = root.get(0).path("typeArgs").get(0).path("type").asText();
    Assert.assertEquals(debug + "map key type", "java.lang.String", val);
    val = root.get(0).path("typeArgs").get(1).path("type").asText();
    Assert.assertEquals(debug + "map value type", "java.lang.Object", val);
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

    JSONObject processingModeAttr = attributesMap.get("PROCESSING_MODE");
    Assert.assertNotNull("processingModeAttr", processingModeAttr);
    Assert.assertEquals("processing mode type", "com.datatorrent.api.Operator$ProcessingMode", processingModeAttr.getString("type"));
    Assert.assertEquals("ui type", "Enum", processingModeAttr.getString("uiType"));
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

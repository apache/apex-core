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
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.stram.webapp.TypeDiscoverer;
import com.google.common.collect.Maps;

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
    Assert.assertEquals("outportList type", "interface java.util.List", val);

    val = root.get(4).path("typeArgs").get(0).path("type").asText();
    Assert.assertEquals("outportList type", "class java.lang.Number", val);
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
    Assert.assertEquals("port type", "class java.lang.String", val);
  }

}

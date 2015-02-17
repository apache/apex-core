/**
 * Copyright (c) 2012-2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.annotate.JsonTypeInfo.As;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.stram.webapp.TypeDiscoverer.UI_TYPE;
import com.google.common.collect.Lists;

public class OperatorDiscoveryTest
{
  @Test
  public void testPropertyDiscovery() throws Exception
  {
    OperatorDiscoverer od = new OperatorDiscoverer();
    Assert.assertNotNull(od.getOperatorClass(BaseOperator.class.getName()));

    JSONObject desc = od.describeClass(TestOperator.class);
    System.out.println("\ntype info for " + TestOperator.class + ":\n" + desc.toString(2));

    JSONArray props = desc.getJSONArray("properties");
    Assert.assertNotNull("properties", props);
    Assert.assertEquals("properties " + props, 13, props.length());
    JSONObject mapProperty = props.getJSONObject(6);
    Assert.assertEquals("name " + mapProperty, "map", mapProperty.get("name"));
    Assert.assertEquals("canGet " + mapProperty, true, mapProperty.get("canGet"));
    Assert.assertEquals("canSet " + mapProperty, true, mapProperty.get("canSet"));
    Assert.assertEquals("type " + mapProperty, java.util.Map.class.getName(), mapProperty.get("type"));

    JSONArray typeArgs = mapProperty.getJSONArray("typeArgs");
    Assert.assertNotNull("typeArgs", typeArgs);
    Assert.assertEquals("typeArgs " + typeArgs, 2, typeArgs.length());
    Assert.assertEquals("", String.class.getName(), typeArgs.getJSONObject(0).get("type"));
    Assert.assertEquals("", Structured.class.getName(), typeArgs.getJSONObject(1).get("type"));

    JSONObject enumDesc = od.describeClass(Color.class);
    JSONArray enumNames = enumDesc.getJSONArray("enum");
    Assert.assertNotNull("enumNames", enumNames);
    Assert.assertEquals("", Color.BLUE.name(), enumNames.get(0));
    JSONArray enumProps = enumDesc.getJSONArray("properties");
    Assert.assertNotNull("properties", enumProps);
    Assert.assertEquals("props " + enumProps, 0, enumProps.length());

    JSONObject structuredProperty = props.getJSONObject(8);
    Assert.assertEquals("name " + structuredProperty, "nested", structuredProperty.get("name"));
    Assert.assertEquals("type " + structuredProperty, Structured.class.getName(), structuredProperty.get("type"));

    JSONObject genericArray = props.getJSONObject(3);
    Assert.assertEquals("name " + genericArray, "genericArray", genericArray.get("name"));
    Assert.assertEquals("type " + genericArray, Object[].class.getName(), genericArray.get("type"));

    JSONObject propProperty = props.getJSONObject(9);
    Assert.assertEquals("uitype " + propProperty, UI_TYPE.MAP.getName(), propProperty.get("uiType"));

    desc = od.describeClass(ExtendedOperator.class);
    props = desc.getJSONArray("properties");
    genericArray = props.getJSONObject(3);
    Assert.assertEquals("name " + genericArray, "genericArray", genericArray.get("name"));
    Assert.assertEquals("type " + genericArray, String[].class.getName(), genericArray.get("type"));

    // type is not a primitive type
    // fetch property meta data to find out how to render it
    desc = od.describeClass(Structured.class);
    System.out.println("\ntype info for " + Structured.class + ":\n" + desc.toString(2));

    desc = od.describeClass(Color.class);
    System.out.println("\ntype info for " + Color.class + ":\n" + desc.toString(2));

  }

  @Test
  public void testFindDescendants() throws Exception
  {
    OperatorDiscoverer od = new OperatorDiscoverer();
    od.includeJRE();

    System.out.println("The descendants list of java type java.util.Map: \n" + od.getDescendants("java.util.Map"));

    System.out.println("The descendants list of java type java.util.List: \n" + od.getDescendants("java.util.List"));

    System.out.println("The descendants list of concrete public type java.util.Map: \n" + od.getPublicConcreteDescendants("java.util.Map", Integer.MAX_VALUE));

    System.out.println("The descendants list of concrete public type java.util.List: \n" + od.getPublicConcreteDescendants("java.util.List", Integer.MAX_VALUE));

    try {
      od.getPublicConcreteDescendants("java.lang.Object", 100);
    } catch (Exception e) {
      Assert.assertEquals("The exception msg: ", "Too many public concrete sub types!", e.getMessage());
    }
  }

  @Test
  public void testValueSerialization() throws Exception
  {
    TestOperator<String> bean = new TestOperator<String>();
    bean.map.put("key1", new Structured());
    bean.stringArray = new String[] { "one", "two", "three" };
    bean.stringList = Lists.newArrayList("four", "five");
    bean.props = new Properties();
    bean.props.setProperty("key1", "value1");
    bean.structuredArray = new Structured[]{new Structured()};
    bean.genericArray = new String[] {"s1"};
    bean.structuredArray[0].name = "s1";
    bean.color = Color.BLUE;
    bean.booleanProp = true;

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    //mapper.enableDefaultTypingAsProperty(ObjectMapper.DefaultTyping.NON_FINAL, Id.CLASS.getDefaultPropertyName());
    mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, As.WRAPPER_OBJECT);
    String s = mapper.writeValueAsString(bean);
    System.out.println(new JSONObject(s).toString(2));


    TestOperator<?> clone = mapper.readValue(s, TestOperator.class);
    Assert.assertNotNull(clone.structuredArray);
    Assert.assertEquals(Color.BLUE, clone.color);
    Assert.assertEquals(bean.structuredArray.length, clone.structuredArray.length);

  }

  public static class Structured
  {
    private int size;
    private String name;
    private ArrayList<String> list;

    public int getSize()
    {
      return size;
    }

    public void setSize(int size)
    {
      this.size = size;
    }

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public ArrayList<String> getList()
    {
      return list;
    }

    public void setList(ArrayList<String> list)
    {
      this.list = list;
    }

  }

  public enum Color
  {
    BLUE,
    RED,
    WHITE
  }

  public static class TestOperator<T> extends BaseOperator
  {
    private int intProp;
    private long longProp;
    private double doubleProp;
    private boolean booleanProp;

    private List<String> stringList;
    private Properties props;
    private Structured nested;
    private Map<String, Structured> map = new HashMap<String, Structured>();
    private String[] stringArray;
    private Color color;
    private Structured[] structuredArray;
    private T[] genericArray;

    public int getIntProp()
    {
      return intProp;
    }

    public void setIntProp(int intProp)
    {
      this.intProp = intProp;
    }

    public long getLongProp()
    {
      return longProp;
    }

    public void setLongProp(long longProp)
    {
      this.longProp = longProp;
    }

    public double getDoubleProp()
    {
      return doubleProp;
    }

    public void setDoubleProp(double doubleProp)
    {
      this.doubleProp = doubleProp;
    }

    public List<String> getStringList()
    {
      return stringList;
    }

    public void setStringList(List<String> stringList)
    {
      this.stringList = stringList;
    }

    public Properties getProps()
    {
      return props;
    }

    public void setProps(Properties props)
    {
      this.props = props;
    }

    public Structured getNested()
    {
      return nested;
    }

    public void setNested(Structured n)
    {
      this.nested = n;
    }

    public Map<String, Structured> getMap()
    {
      return map;
    }

    public void setMap(Map<String, Structured> m)
    {
      this.map = m;
    }

    public Color getColor()
    {
      return color;
    }

    public void setColor(Color color)
    {
      this.color = color;
    }

    public String[] getStringArray()
    {
      return stringArray;
    }

    public Structured[] getStructuredArray()
    {
      return structuredArray;
    }

    public void setStructuredArray(Structured[] structuredArray)
    {
      this.structuredArray = structuredArray;
    }

    public T[] getGenericArray()
    {
      return genericArray;
    }

    public void setGenericArray(T[] genericArray)
    {
      this.genericArray = genericArray;
    }

    public boolean isBooleanProp() {
      return booleanProp;
    }

    public void setBooleanProp(boolean booleanProp) {
      this.booleanProp = booleanProp;
    }
  }

  static class ExtendedOperator extends TestOperator<String>
  {
  }

}

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
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.codehaus.jackson.annotate.JsonTypeInfo.As;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.stram.webapp.TypeDiscoverer.UI_TYPE;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class OperatorDiscoveryTest
{
  @Test
  public void testPropertyDiscovery() throws Exception
  {
    OperatorDiscoverer od = new OperatorDiscoverer();
    od.includeJRE();
    od.includeCurrentClasspathLibrary();
    Assert.assertNotNull(od.getOperatorClass(BaseOperator.class.getName()));

    JSONObject desc = od.describeClass(TestOperator.class);
    System.out.println("\ntype info for " + TestOperator.class + ":\n" + desc.toString(2));
    
    JSONObject asmDesc = od.describeClassByASM(TestOperator.class.getName());
    System.out.println("\n(ASM)type info for " + TestOperator.class + ":\n" + asmDesc.toString(2));

    JSONArray props = desc.getJSONArray("properties");
    Assert.assertNotNull("properties", props);
    Assert.assertEquals("properties " + props, 15, props.length());
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
    
    desc = od.describeClass(Properties.class);
    System.out.println("\ntype info for " + Properties.class + ":\n" + desc.toString(2));
    
    desc = od.describeClass(HashMap.class);
    System.out.println("\ntype info for " + HashMap.class + ":\n" + desc.toString(2));
    
    
    System.out.println("\n(ASM)type info for " + Color.class + ":\n" + od.describeClassByASM(Color.class.getName()).toString(2));
    
    System.out.println("\n(ASM)type info for " + Structured.class + ":\n" + od.describeClassByASM(Structured.class.getName()).toString(2));

  }

  @Test
  public void testFindDescendants() throws Exception
  {
    OperatorDiscoverer od = new OperatorDiscoverer();
    od.includeJRE();
    od.includeCurrentClasspathLibrary();
    System.out.println("The descendants list of java type java.util.Map: \n" + od.getDescendants("java.util.Map"));

    System.out.println("The descendants list of java type java.util.List: \n" + od.getDescendants("java.util.List"));

    System.out.println("The initializable descendants list of type java.util.Map: \n" + od.getInitializableDescendants("java.util.Map", Integer.MAX_VALUE));

    System.out.println("The initializable descendants list of type java.util.List: \n" + od.getInitializableDescendants("java.util.List", Integer.MAX_VALUE));
    
    System.out.println("The initializable descendants list of type com.google.common.collect.Multimap: \n" + od.getInitializableDescendants("com.google.common.collect.Multimap", Integer.MAX_VALUE));
    
    
    Set<String> actualQueueClass = Sets.newHashSet();
    String[] jdkQueue = new String[] {DelayQueue.class.getName(), LinkedBlockingDeque.class.getName(), 
        LinkedBlockingQueue.class.getName(), PriorityBlockingQueue.class.getName(), SynchronousQueue.class.getName()};
    JSONArray queueJsonArray = od.getInitializableDescendants("java.util.concurrent.BlockingQueue", Integer.MAX_VALUE);
    // at lease include all the classes in jdk
    System.out.println(queueJsonArray);
    Assert.assertTrue("All the queue class in jdk are expected in result ", queueJsonArray.length() >= jdkQueue.length);
    for (int i = 0; i < queueJsonArray.length(); i++) {
      actualQueueClass.add(queueJsonArray.getString(i));
    }
    for (String expectedClass : jdkQueue) {
      Assert.assertTrue("Actual queue set should contain any one of the expected class ", actualQueueClass.contains(expectedClass));
    }
    System.out.println("The initializable descendants of type java.util.concurrent.BlockingQueue: \n" + od.getInitializableDescendants("java.util.concurrent.BlockingQueue", Integer.MAX_VALUE));

    try {
      od.getInitializableDescendants("java.lang.Object", 100);
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
    private Map<String, List<Map<String, Number>>> zaMap = new HashMap<String, List<Map<String, Number>>>();
    private Map<? extends Object, ? super Long> zbMap = new HashMap<Object, Number>();

    
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

    public Map<String, List<Map<String, Number>>> getZaMap()
    {
      return zaMap;
    }

    public void setZaMap(Map<String, List<Map<String, Number>>> zaMap)
    {
      this.zaMap = zaMap;
    }


    public Map<? extends Object, ? super Long> getZbMap()
    {
      return zbMap;
    }

    public void setZbMap(Map<? extends Object, ? super Long> zbMap)
    {
      this.zbMap = zbMap;
    }

  }

  static class ExtendedOperator extends TestOperator<String>
  {
  }

}

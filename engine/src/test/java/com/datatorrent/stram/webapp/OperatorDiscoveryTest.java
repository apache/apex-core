/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.webapp;

import java.io.File;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.tools.ant.DirectoryScanner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.util.ObjectMapperFactory;
import com.datatorrent.stram.webapp.TypeDiscoverer.UI_TYPE;
import com.google.common.collect.Lists;

public class OperatorDiscoveryTest
{
//  private static final Logger LOG = LoggerFactory.getLogger(OperatorDiscoveryTest.class);
  
  public static class GenericClassBase<T> extends BaseOperator
  {
    private int A;
    private T B;
    @InputPortFieldAnnotation(optional = true)
    public transient final DefaultInputPort<T> input = new DefaultInputPort<T>() {
      @Override
      public void process(T tuple) {
        output.emit("abcd");
      }
    };

    public transient final DefaultInputPort<T> input1 = new DefaultInputPort<T>() {
      public void process(T tuple) {
        // Do nothing
      }
    };

    @OutputPortFieldAnnotation(optional = false, error= true)
    public transient final DefaultOutputPort<String> output = new DefaultOutputPort<String>();

    public transient final DefaultOutputPort<Double> output1 = new DefaultOutputPort<Double>();

    public String getName()
    {
      return "abc";
    }

    public int getA() {
      return A;
    }

    public void setA(int a) {
      A = a;
    }

    public T getB() {
      return B;
    }

    public void setB(T b) {
      B = b;
    }
  }

  public static class SubClassGeneric<K extends Number> extends GenericClassBase<K>
  {

  }

  public static class SubSubClassGeneric<T extends Long> extends SubClassGeneric<T>
  {

  }

  @Test
  public void testOperatorDiscoverer() throws Exception
  {
    String[] classFilePath = getClassFileInClasspath();
    OperatorDiscoverer operatorDiscoverer = new OperatorDiscoverer(classFilePath);
    operatorDiscoverer.buildTypeGraph();
    JSONObject oper = operatorDiscoverer.describeOperator(SubSubClassGeneric.class);
    System.out.println(oper);
    String debug = "\n(ASM)type info for " + TestOperator.class + ":\n" + oper.toString(2) + "\n";

    JSONArray props = oper.getJSONArray("properties");
    JSONArray portTypes = oper.getJSONArray("portTypeInfo");
    JSONArray inputPorts = oper.getJSONArray("inputPorts");
    JSONArray outputPorts = oper.getJSONArray("outputPorts");

    Assert.assertNotNull(debug + "Properties aren't null ", props);
    Assert.assertEquals(debug + "Number of properties ", 3, props.length());

    Assert.assertNotNull(debug + "Port types aren't null ", portTypes);
    Assert.assertEquals(debug + "Number of port types ", 4, portTypes.length());

    Assert.assertNotNull(debug + "inputPorts aren't null ", inputPorts);
    Assert.assertEquals(debug + "Number of inputPorts ", 2, inputPorts.length());

    Assert.assertNotNull(debug + "outputPorts aren't null ", outputPorts);
    Assert.assertEquals(debug + "Number of outputPorts ", 2, outputPorts.length());

    // Validate port types
    JSONObject portType = (JSONObject)portTypes.get(0);
    Assert.assertEquals(portType.get("name"), "input");
    Assert.assertEquals(portType.get("typeLiteral"), "T");
    Assert.assertEquals(portType.get("type"), "java.lang.Long");

    portType = (JSONObject)portTypes.get(2);
    Assert.assertEquals(portType.get("name"), "output");
    Assert.assertEquals(portType.get("type"), "java.lang.String");

    // Validate input port annotations
    JSONObject inputPort = (JSONObject)inputPorts.get(0);
    Assert.assertEquals(inputPort.get("name"), "input");
    Assert.assertEquals(inputPort.get("optional"), true);

    inputPort = (JSONObject)inputPorts.get(1);
    Assert.assertEquals(inputPort.get("name"), "input1");
    Assert.assertEquals(inputPort.get("optional"), false);

    // Validate output port annotations
    JSONObject outPort = (JSONObject)outputPorts.get(0);
    Assert.assertEquals(outPort.get("name"), "output");
    Assert.assertEquals(outPort.get("optional"), false);
    Assert.assertEquals(outPort.get("error"), true);

    outPort = (JSONObject)outputPorts.get(1);
    Assert.assertEquals(outPort.get("name"), "output1");
    Assert.assertEquals(outPort.get("optional"), true);
    Assert.assertEquals(outPort.get("error"), false);
  }

  @Test
  public void testPropertyDiscovery() throws Exception
  {
    
    String[] classFilePath = getClassFileInClasspath();
    OperatorDiscoverer od = new OperatorDiscoverer(classFilePath);
    od.buildTypeGraph();

    Assert.assertNotNull(od.getOperatorClass(BaseOperator.class.getName()));
    Assert.assertFalse("Base Operator is not instantiable because it is not an InputOperator and it has no input port ",
            OperatorDiscoverer.isInstantiableOperatorClass(BaseOperator.class));

    JSONObject asmDesc = od.describeClassByASM(TestOperator.class.getName());
    String debug = "\n(ASM)type info for " + TestOperator.class + ":\n" + asmDesc.toString(2) + "\n";

    JSONArray props = asmDesc.getJSONArray("properties");
    Assert.assertNotNull(debug + "Properties aren't null ", props);
    Assert.assertEquals(debug + "Number of properties ", 27, props.length());

    JSONObject mapProperty = getJSONProperty(props, "map");
    Assert.assertEquals(debug + "canGet " + mapProperty, true, mapProperty.get("canGet"));
    Assert.assertEquals(debug + "canSet " + mapProperty, true, mapProperty.get("canSet"));
    Assert.assertEquals(debug + "type " + mapProperty, java.util.Map.class.getName(), mapProperty.get("type"));

    JSONArray typeArgs = mapProperty.getJSONArray("typeArgs");
    Assert.assertNotNull(debug + "typeArgs of map is not null", typeArgs);
    Assert.assertEquals(debug + "number of typeArgs of map ", 2, typeArgs.length());
    Assert.assertEquals(debug + "The first typeArg of map", String.class.getName(), typeArgs.getJSONObject(0).get("type"));
    Assert.assertEquals(debug + "The second typeArg of map", Structured.class.getName(), typeArgs.getJSONObject(1).get("type"));

    JSONObject structuredProperty = getJSONProperty(props, "nested");
    Assert.assertEquals(debug + "type " + structuredProperty, Structured.class.getName(), structuredProperty.get("type"));

    JSONObject genericArray = getJSONProperty(props, "genericArray");
    Assert.assertEquals(debug + "type " + genericArray, Object[].class.getName(), genericArray.get("type"));

    JSONObject propProperty = getJSONProperty(props, "props");
    Assert.assertEquals(debug + "uitype " + propProperty, UI_TYPE.MAP.getName(), propProperty.get("uiType"));

    JSONObject stringArrayProperty = getJSONProperty(props, "stringArray");
    Assert.assertEquals(debug + "type " + stringArrayProperty, String[].class.getName(), stringArrayProperty.get("type"));

    JSONObject nestedParameterizedTypeProperpty = getJSONProperty(props, "nestedParameterizedType");
    Assert.assertEquals(debug + "type " + nestedParameterizedTypeProperpty, Map.class.getName(), nestedParameterizedTypeProperpty.get("type"));
    Assert.assertEquals(debug + "type " + nestedParameterizedTypeProperpty, Number.class.getName(),
        nestedParameterizedTypeProperpty.getJSONArray("typeArgs").getJSONObject(1).getJSONArray("typeArgs").getJSONObject(0).getJSONArray("typeArgs").getJSONObject(1).get("type"));

    JSONObject wildcardType = getJSONProperty(props, "wildcardType");
    Assert.assertEquals(debug + "type " + wildcardType, Map.class.getName(), wildcardType.get("type"));
    Assert.assertEquals(debug + "type " + wildcardType, "class " + Long.class.getName(),
        wildcardType.getJSONArray("typeArgs").getJSONObject(1).getJSONObject("typeBounds").getJSONArray("lower").get(0));

    JSONObject multiDimensionPrimitiveArray = getJSONProperty(props, "multiDimensionPrimitiveArray");
    Assert.assertEquals(debug + "type " + multiDimensionPrimitiveArray, int[][].class.getName(), multiDimensionPrimitiveArray.get("type"));


    JSONObject enumDesc = od.describeClass(Color.class);
    debug = "\nJson for Color enum:\n" + enumDesc.toString(2) + "\n";
    JSONArray enumNames = enumDesc.getJSONArray("enum");
    Assert.assertNotNull(debug + "enumNames are not null", enumNames);
    Assert.assertEquals(debug + "First element of color", Color.BLUE.name(), enumNames.get(0));

    JSONObject desc = od.describeClass(ExtendedOperator.class);
    debug = "\ntype info for " + ExtendedOperator.class + ":\n" + desc.toString(2) + "\n";
    props = desc.getJSONArray("properties");
    genericArray = getJSONProperty(props, "genericArray");
    Assert.assertEquals(debug + "type " + genericArray, String[].class.getName(), genericArray.get("type"));
    
    
    // Test complicated Type Variable override in Hierarchy
    desc = od.describeClassByASM(SubSubClass.class.getName());
    props = desc.getJSONArray("properties");
    JSONObject aObj = getJSONProperty(props, "a");
    Assert.assertEquals("type " + aObj, Number.class.getName(), aObj.get("type"));
    JSONObject bObj = getJSONProperty(props, "b");
    Assert.assertEquals("type " + bObj, "long", bObj.get("type"));
    JSONObject cObj = getJSONProperty(props, "c");
    Assert.assertEquals("type " + cObj, List.class.getName(), cObj.get("type"));
    JSONObject dObj = getJSONProperty(props, "d");
    Assert.assertEquals("type " + dObj, List.class.getName(), dObj.get("type"));
    JSONObject eObj = getJSONProperty(props, "e");
    Assert.assertEquals("type " + eObj, Runnable.class.getName(), eObj.get("type"));

    // describeClassByASM now populates portTypes too, so checking only properties part
    ObjectMapper om = new ObjectMapper();
    desc = od.describeClass(Structured.class);
    asmDesc = od.describeClassByASM(Structured.class.getName());
    Assert.assertEquals("\ntype info for " + Structured.class + ":\n",  om.readTree(desc.get("properties").toString()), om.readTree(asmDesc.get("properties").toString()));

    desc = od.describeClass(Color.class);
    asmDesc = od.describeClassByASM(Color.class.getName());
    Assert.assertEquals("\ntype info for " + Color.class + ":\n", om.readTree(desc.get("properties").toString()), om.readTree(asmDesc.get("properties").toString()));

  }

  public static String[] getClassFileInClasspath()
  {
    String classpath = System.getProperty("java.class.path");
    String[] paths = classpath.split(":");
    List<String> fnames = new LinkedList<String>();
    for (String cp : paths) {
      File f = new File(cp);
      if(!f.isDirectory()){
        continue;
      }
      DirectoryScanner ds = new DirectoryScanner();
      ds.setBasedir(f);
      ds.setIncludes(new String[] { "**\\*.class" });
      ds.scan();
      for (String name : ds.getIncludedFiles()) {
        fnames.add(new File(f, name).getAbsolutePath());
      }

    }
    return fnames.toArray(new String[]{});
  }

  private JSONObject getJSONProperty(JSONArray props, String name) throws JSONException
  {
    for (int i = 0; i < props.length(); i++) {
      if(props.getJSONObject(i).get("name").equals(name)){
        return props.getJSONObject(i);
      }
    }
    return null;
  }

  @Test
  public void testFindDescendants() throws Exception
  {
    OperatorDiscoverer od = new OperatorDiscoverer();


    List<String> dList = od.getTypeGraph().getInitializableDescendants("java.util.Map");
    Assert.assertTrue("The initializable descendants list of type java.util.Map: \n" + dList, dList.contains("java.util.HashMap"));

    dList = od.getTypeGraph().getInitializableDescendants("java.util.List");
    Assert.assertTrue("The initializable descendants list of type java.util.List: \n" + dList, dList.contains("java.util.ArrayList"));

    dList = od.getTypeGraph().getInitializableDescendants("java.util.HashMap");
    Assert.assertTrue("The initializable descendants list of type java.util.HashMap: \n" + dList, dList.contains("java.util.HashMap"));


    String[] jdkQueue = new String[] {DelayQueue.class.getName(), LinkedBlockingDeque.class.getName(),
        LinkedBlockingQueue.class.getName(), PriorityBlockingQueue.class.getName(), SynchronousQueue.class.getName()};
    List<String> actualQueueClass = od.getTypeGraph().getInitializableDescendants("java.util.concurrent.BlockingQueue");


    for (String expectedClass : jdkQueue) {
      Assert.assertTrue("Actual queue: " + actualQueueClass.toString() + "\n Expected contained queue: " + expectedClass, actualQueueClass.contains(expectedClass));
    }

  }

  @Test
  public void testValueSerialization() throws Exception
  {
    TestOperator<String, Map<String, Number>> bean = new TestOperator<String, Map<String, Number>>();
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
    bean.nestedList = new LinkedList<OperatorDiscoveryTest.Structured>();
    Structured st = new Structured();
    st.name = "nestedone";
    st.size = 10;
    bean.nestedList.add(st);
    bean.uri = new URI("file:///tmp/file");
    bean.integerProp = 44;


    ObjectMapper mapper = ObjectMapperFactory.getOperatorValueSerializer();
    String s = mapper.writeValueAsString(bean);
    JSONObject jsonObj = new JSONObject(s);
    Assert.assertTrue("Null property 'nested' should be cut off", !jsonObj.has("nested"));


    TestOperator<?, ?> clone = mapper.readValue(s, TestOperator.class);
    Assert.assertNotNull(clone.structuredArray);
    Assert.assertEquals(Color.BLUE, clone.color);
    Assert.assertEquals(bean.structuredArray.length, clone.structuredArray.length);
    Assert.assertEquals(bean.integerProp, clone.integerProp);
    Assert.assertEquals(bean.uri, clone.uri);


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

    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((list == null) ? 0 : list.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + size;
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Structured other = (Structured) obj;
      if (list == null) {
        if (other.list != null)
          return false;
      } else if (!list.equals(other.list))
        return false;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      if (size != other.size)
        return false;
      return true;
    }
    
    
    

  }

  public enum Color
  {
    BLUE,
    RED,
    WHITE
  }

  public static class TestOperator<T, Z extends Map<String, Number>> extends BaseOperator
  {
    private int intProp;
    private long longProp;
    private double doubleProp;
    private boolean booleanProp;
    
    private Integer integerProp;
    private List<String> stringList;
    private List<Structured> nestedList;
    private Properties props;
    private Structured nested;
    private Map<String, Structured> map = new HashMap<String, Structured>();
    private String[] stringArray;
    private Color color;
    private Structured[] structuredArray;
    private T[] genericArray;
    private Map<String, List<Map<String, Number>>> nestedParameterizedType = new HashMap<String, List<Map<String, Number>>>();
    private Map<? extends Object, ? super Long> wildcardType = new HashMap<Object, Number>();
    private List<int[]> listofIntArray = new LinkedList<int[]>();
    private List<T> parameterizedTypeVariable = new LinkedList<T>();
    private Z genericType;
    private int[][] multiDimensionPrimitiveArray;
    private Structured[][] multiDimensionComplexArray;
    private URI uri;
    private String realName = "abc";
    private String getterOnly = "getterOnly";
    
    // this property can not be deserialized by jackson but it will be ignored if it has no setter method
    private Map<Class, String> mProp;
    
    
    public Map<Class, String> getmProp()
    {
      return mProp;
    }
    
    public String getAlias()
    {
      return realName;
    }
    
    public void setAlias(String alias)
    {
      realName = alias;
    }
    
    public String getGetterOnly()
    {
      return getterOnly;
    }
    
    
    public URI getUri()
    {
      return uri;
    }
    
    public void setUri(URI uri)
    {
      this.uri = uri;
    }
    
    
    public void setIntegerProp(Integer integerProp)
    {
      this.integerProp = integerProp;
    }
    
    public Integer getIntegerProp()
    {
      return integerProp;
    }

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
    
    public void setStringArray(String[] stringArray)
    {
      this.stringArray = stringArray;
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

    public Map<String, List<Map<String, Number>>> getNestedParameterizedType()
    {
      return nestedParameterizedType;
    }

    public void setNestedParameterizedType(Map<String, List<Map<String, Number>>> nestedParameterizedType)
    {
      this.nestedParameterizedType = nestedParameterizedType;
    }


    public Map<? extends Object, ? super Long> getWildcardType()
    {
      return wildcardType;
    }

    public void setWildcardType(Map<? extends Object, ? super Long> wildcardType)
    {
      this.wildcardType = wildcardType;
    }

    public Z getGenericType()
    {
      return genericType;
    }

    public void setGenericType(Z genericType)
    {
      this.genericType = genericType;
    }

    public int[][] getMultiDimensionPrimitiveArray()
    {
      return multiDimensionPrimitiveArray;
    }

    public void setMultiDimensionPrimitiveArray(int[][] multiDimensionPrimitiveArray)
    {
      this.multiDimensionPrimitiveArray = multiDimensionPrimitiveArray;
    }

    public Structured[][] getMultiDimensionComplexArray()
    {
      return multiDimensionComplexArray;
    }

    public void setMultiDimensionComplexArray(Structured[][] multiDimensionComplexArray)
    {
      this.multiDimensionComplexArray = multiDimensionComplexArray;
    }

    public List<int[]> getListofIntArray()
    {
      return listofIntArray;
    }

    public void setListofIntArray(List<int[]> listofIntArray)
    {
      this.listofIntArray = listofIntArray;
    }

    public List<T> getParameterizedTypeVariable()
    {
      return parameterizedTypeVariable;
    }

    public void setParameterizedTypeVariable(List<T> parameterizedTypeVariable)
    {
      this.parameterizedTypeVariable = parameterizedTypeVariable;
    }

    public <AMAZING extends Callable<Map<String, String>>> AMAZING getAmazing(){
      return null;
    }

    public List<Structured> getNestedList()
    {
      return nestedList;
    }

    public void setNestedList(List<Structured> nestedList)
    {
      this.nestedList = nestedList;
    }

  }

  static class ExtendedOperator extends TestOperator<String, Map<String, Number>>
  {
  }
  
  public static class BaseClass<A, B, C>
  {
    private A a;
    
    private B b;

    private C c;
    
    public void setA(A a)
    {
      this.a = a;
    }
    public void setB(B b)
    {
      this.b = b;
    }
    
    public A getA()
    {
      return a;
    }
    
    public B getB()
    {
      return b;
    }

    public void setC(C c)
    {
      this.c = c;
    }
    
    public C getC()
    {
      return c;
    }
  }

  public static class SubClass<D, A extends Number> extends BaseClass<Number, A, D>
  {
    private D d;
    
    public void setD(D d)
    {
      this.d = d;
    }
    
    public D getD()
    {
      return d;
    }
    
  }

  public static class SubSubClass<E extends Runnable> extends SubClass<List<String>, Long>
  {
    private E e;
    
    public void setE(E e)
    {
      this.e = e;
    }
    
    public E getE()
    {
      return e;
    }
  }

  public static class ArraysHolder
  {
    public int[] intArray =  new int[] { 1, 2, 3 };
    public Structured[] beanArray = new Structured[] {};
    public int[] getIntArray()
    {
      return intArray;
    }
    public void setIntArray(int[] intArray)
    {
      this.intArray = intArray;
    }
    public Structured[] getBeanArray()
    {
      return beanArray;
    }
    public void setBeanArray(Structured[] beanArray)
    {
      this.beanArray = beanArray;
    }
  }

  @Test
  public void testArraySerialization() throws Exception
  {
    OperatorDiscoverer od = new OperatorDiscoverer();
    Assert.assertNotNull(od.getOperatorClass(BaseOperator.class.getName()));
    JSONObject desc = od.describeClass(ArraysHolder.class);
    String debugInfo = "\ntype info for " + ArraysHolder.class + ":\n" + desc.toString(2) + "\n";

    JSONArray props = desc.getJSONArray("properties");
    ArraysHolder ah = new ArraysHolder();

    JSONObject beanArray = getJSONProperty(props, "beanArray");
    Assert.assertEquals(debugInfo + "type " + ah.beanArray.getClass(), ah.beanArray.getClass().getName(), beanArray.get("type"));

    JSONObject intArray = getJSONProperty(props, "intArray");
    Assert.assertEquals(debugInfo + "type " + ah.intArray.getClass(), ah.intArray.getClass().getName(), intArray.get("type"));

    ObjectMapper mapper = ObjectMapperFactory.getOperatorValueSerializer();
    String s = mapper.writeValueAsString(ah);

    ArraysHolder clone = mapper.readValue(s, ArraysHolder.class);
    Assert.assertNotNull(clone.intArray);
    Assert.assertArrayEquals(ah.intArray, clone.intArray);

  }
  
  @Test
  public void testLogicalPlanConfiguration() throws Exception
  {
    TestOperator<String, Map<String, Number>> bean = new TestOperator<String, Map<String, Number>>();
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
    bean.realName = "abc";

    ObjectMapper mapper = ObjectMapperFactory.getOperatorValueSerializer();
    String s = mapper.writeValueAsString(bean);
//    LOG.debug(new JSONObject(s).toString(2));
    // 
    Assert.assertTrue("Shouldn't contain field 'realName' !", !s.contains("realName"));
    Assert.assertTrue("Should contain property 'alias' !", s.contains("alias"));
    Assert.assertTrue("Shouldn't contain property 'getterOnly' !", !s.contains("getterOnly"));
    JSONObject jsonObj = new JSONObject(s);
    
    // create the json dag representation 
    JSONObject jsonPlan = new JSONObject();
    jsonPlan.put("streams", new JSONArray());
    JSONObject jsonOper = new JSONObject();
    jsonOper.put("name", "Test Operator");
    jsonOper.put("class", TestOperator.class.getName());
    jsonOper.put("properties", jsonObj);
    jsonPlan.put("operators", new JSONArray(Lists.newArrayList(jsonOper)));
    
    
    Configuration conf = new Configuration(false);
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    // create logical plan from the json 
    LogicalPlan lp = lpc.createFromJson(jsonPlan, "jsontest");
    OperatorMeta om = lp.getOperatorMeta("Test Operator");
    Assert.assertTrue(om.getOperator() instanceof TestOperator);
    @SuppressWarnings("rawtypes")
    TestOperator beanBack = (TestOperator) om.getOperator();
    
    // The operator deserialized back from json should be same as original operator
    Assert.assertEquals(bean.map, beanBack.map);
    Assert.assertArrayEquals(bean.stringArray, beanBack.stringArray);
    Assert.assertEquals(bean.stringList, beanBack.stringList);
    Assert.assertEquals(bean.props, beanBack.props);
    Assert.assertArrayEquals(bean.structuredArray, beanBack.structuredArray);
    Assert.assertArrayEquals(bean.genericArray, beanBack.genericArray);
    Assert.assertEquals(bean.color, beanBack.color);
    Assert.assertEquals(bean.booleanProp, beanBack.booleanProp);
    Assert.assertEquals(bean.realName, beanBack.realName);
    Assert.assertEquals(bean.getterOnly, beanBack.getterOnly);
    
    
  }

}

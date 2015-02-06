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

import org.codehaus.jackson.annotate.JsonTypeInfo.Id;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.BaseOperator;
import com.google.common.collect.Lists;

public class OperatorDiscoveryTest
{
  @Test
  public void testPropertyDiscovery() throws Exception
  {
    OperatorDiscoverer od = new OperatorDiscoverer();
    Assert.assertNotNull(od.getOperatorClass(BaseOperator.class.getName()));

    JSONArray arr = od.getClassProperties(CustomBean.class, 0);
    System.out.println(arr.toString(2));

  }

  @Test
  public void testValueSerialization() throws Exception
  {
    CustomBean bean = new CustomBean();
    bean.map.put("key1", new CustomBean.Nested());
    bean.stringArray = new String[] { "one", "two", "three" };
    bean.stringList = Lists.newArrayList("four", "five");

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.enableDefaultTypingAsProperty(ObjectMapper.DefaultTyping.NON_FINAL, Id.CLASS.getDefaultPropertyName());
    //mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
    String s = mapper.writeValueAsString(bean);
    System.out.println(new JSONObject(s).toString(2));
  }

  public static class CustomBean
  {
    private int count;
    private List<String> stringList;
    private Properties props;
    private Nested nested;
    private Map<String, Nested> map = new HashMap<String, CustomBean.Nested>();
    private String[] stringArray;

    public static class Nested
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

    public int getCount()
    {
      return count;
    }

    public void setCount(int count)
    {
      this.count = count;
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

    public Nested getNested()
    {
      return nested;
    }

    public void setNested(Nested n)
    {
      this.nested = n;
    }

    public Map<String, Nested> getMap()
    {
      return map;
    }

    public void setMap(Map<String, Nested> m)
    {
      this.map = m;
    }

    public String[] getStringArray()
    {
      return stringArray;
    }


  }

}

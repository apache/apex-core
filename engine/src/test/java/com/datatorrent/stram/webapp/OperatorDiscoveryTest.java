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

import org.codehaus.jettison.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.BaseOperator;

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

  public static class CustomBean
  {
    private int count;
    private List<String> stringList;
    private Properties props;
    private Nested n;
    public Map<String, Nested> m = new HashMap<String, CustomBean.Nested>();

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

    public Nested getN()
    {
      return n;
    }

    public void setN(Nested n)
    {
      this.n = n;
    }

    public Map<String, Nested> getM()
    {
      return m;
    }

    public void setM(Map<String, Nested> m)
    {
      this.m = m;
    }

  }

}

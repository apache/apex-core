/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.client;

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */


public class EvalPropertiesTest
{
  @Test
  public void testEvalExpression() throws Exception
  {
    Configuration conf = new Configuration();
    conf.set("a.b.c", "123");
    conf.set("d.e.f", "456");
    conf.set("x.y.z", "foobar");

    Properties prop = new Properties();
    prop.put("product.result", "Product result is {% (_prop[\"a.b.c\"] * _prop[\"d.e.f\"]).toFixed(0) %}...");
    prop.put("concat.result", "Concat result is {% _prop[\"x.y.z\"] %} ... {% _prop[\"a.b.c\"] %} blah");

    StramClientUtils.evalProperties(prop, conf);

    Assert.assertEquals("Product result is " + (123 * 456) + "...", prop.get("product.result"));
    Assert.assertEquals("Concat result is foobar ... 123 blah", prop.get("concat.result"));
    Assert.assertEquals("123", conf.get("a.b.c"));
    Assert.assertEquals("456", conf.get("d.e.f"));
    Assert.assertEquals("foobar", conf.get("x.y.z"));
  }

  @Test
  public void testVariableSubstitution() throws Exception
  {
    Configuration conf = new Configuration();
    conf.set("a.b.c", "123");
    conf.set("x.y.z", "foobar");

    Properties prop = new Properties();
    prop.put("var.result", "1111 ${a.b.c} xxx ${x.y.z} yyy");

    StramClientUtils.evalProperties(prop, conf);

    Assert.assertEquals("1111 123 xxx foobar yyy", prop.get("var.result"));
  }
}

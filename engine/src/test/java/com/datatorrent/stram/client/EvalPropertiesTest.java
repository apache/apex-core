/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.client;

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
    DTConfiguration conf = new DTConfiguration();
    conf.set("a.b.c", "123", DTConfiguration.Scope.TRANSIENT, null);
    conf.set("d.e.f", "456", DTConfiguration.Scope.TRANSIENT, null);
    conf.set("x.y.z", "foobar", DTConfiguration.Scope.TRANSIENT, null);

    conf.set("product.result", "Product result is {% (_prop[\"a.b.c\"] * _prop[\"d.e.f\"]).toFixed(0) %}...", DTConfiguration.Scope.TRANSIENT, null);
    conf.set("concat.result", "Concat result is {% _prop[\"x.y.z\"] %} ... {% _prop[\"a.b.c\"] %} blah", DTConfiguration.Scope.TRANSIENT, null);

    StramClientUtils.evalProperties(conf);

    Assert.assertEquals("Product result is " + (123 * 456) + "...", conf.get("product.result"));
    Assert.assertEquals("Concat result is foobar ... 123 blah", conf.get("concat.result"));
    Assert.assertEquals("123", conf.get("a.b.c"));
    Assert.assertEquals("456", conf.get("d.e.f"));
    Assert.assertEquals("foobar", conf.get("x.y.z"));
  }
}

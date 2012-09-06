/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.math;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.Sink;

/**
 *
 */
public class TestArithmeticSum {

  class TestSink implements Sink {
    List<Object> collectedTuples = new ArrayList<Object>();

    @Override
    public void process(Object payload) {
      collectedTuples.add(payload);
    }

  }


  /**
   * Test configuration and parameter validation of the node
   */
  @Test
  public void testNodeValidation() {

    NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());

    ArithmeticQuotient node = new ArithmeticQuotient();


    conf.set(ArithmeticQuotient.KEY_MULTIPLY_BY, "junk");

    try {
      node.checkConfiguration(conf);
      Assert.fail("validation error  " + ArithmeticQuotient.KEY_MULTIPLY_BY);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + ArithmeticQuotient.KEY_MULTIPLY_BY, e.getMessage().contains("expectedErrorSubString"));
    }

  }

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() {

    ArithmeticQuotient node = new ArithmeticQuotient();

    TestSink quotientSink = new TestSink();
    node.connect(ArithmeticQuotient.OPORT_QUOTIENT, quotientSink);

    HashMap<String, String> input = new HashMap<String, String>();
    node.process(input);
    node.endWindow();
    Assert.assertEquals("number emitted tuples", 1, quotientSink.collectedTuples.size());
    Assert.assertEquals("emitted tuple", "testtest", quotientSink.collectedTuples.get(0));

  }

}

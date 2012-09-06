/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.testbench;

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
public class TestLoadGenerator {

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

    LoadGenerator node = new LoadGenerator();


    conf.set(LoadGenerator.KEY_KEYS, "a,b,c,d");
    conf.set(LoadGenerator.KEY_VALUES, "1,2,3,4");
    conf.set(LoadGenerator.KEY_WEIGHTS, "10,40,20,30");
    conf.set(LoadGenerator.KEY_TUPLES_PER_MS, "100");

    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + LoadGenerator.KEY_KEYS);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + LoadGenerator.KEY_KEYS, e.getMessage().contains("expectedErrorSubString"));
    }
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() {

    LoadGenerator node = new LoadGenerator();

    TestSink lgenSink = new TestSink();
    node.connect(LoadGenerator.OPORT_DATA, lgenSink);

    HashMap<String, String> input = new HashMap<String, String>();
    node.process(input);
    node.endWindow();
    Assert.assertEquals("number emitted tuples", 1, lgenSink.collectedTuples.size());
    Assert.assertEquals("emitted tuple", "testtest", lgenSink.collectedTuples.get(0));

  }

}

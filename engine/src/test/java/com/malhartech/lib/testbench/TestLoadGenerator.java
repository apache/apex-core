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
import com.malhartech.lib.math.ArithmeticQuotient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestLoadGenerator {
  private static Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);
  
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

    conf.set(LoadGenerator.KEY_KEYS, "");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + LoadGenerator.KEY_KEYS);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + LoadGenerator.KEY_KEYS,
                        e.getMessage().contains("is empty"));
    }  
 
    conf.set(LoadGenerator.KEY_KEYS, "a,b,c,d"); // from now on keys would be a,b,c,d
    conf.set(LoadGenerator.KEY_WEIGHTS, "10.4,40,20,30");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + LoadGenerator.KEY_WEIGHTS);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + LoadGenerator.KEY_WEIGHTS,
                        e.getMessage().contains("should be an integer"));
    }
    
    conf.set(LoadGenerator.KEY_WEIGHTS, "10,40,20"); // from now on weights would be 10,40,20,30
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + LoadGenerator.KEY_WEIGHTS);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + LoadGenerator.KEY_WEIGHTS,
                        e.getMessage().contains("does not match number of keys"));
    }    

    conf.set(LoadGenerator.KEY_WEIGHTS, ""); // from now on weights would be 10,40,20,30
    conf.set(LoadGenerator.KEY_VALUES, "a,2,3,4");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + LoadGenerator.KEY_VALUES);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + LoadGenerator.KEY_VALUES,
                        e.getMessage().contains("should be float"));
    }

    conf.set(LoadGenerator.KEY_WEIGHTS, "10,40,30,20"); // from now on weights would be 10,40,20,30
    conf.set(LoadGenerator.KEY_VALUES, "1,2,3");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + LoadGenerator.KEY_VALUES);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + LoadGenerator.KEY_VALUES,
                        e.getMessage().contains("does not match number of keys"));
    }    
    
    conf.set(LoadGenerator.KEY_VALUES, "1,2,3,4");
    conf.set(LoadGenerator.KEY_TUPLES_PER_MS, "0");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + LoadGenerator.KEY_VALUES);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + LoadGenerator.KEY_VALUES,
                        e.getMessage().contains("has to be > 0"));
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


  }

}

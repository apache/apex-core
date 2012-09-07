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
public class TestLoadClassifier {

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

    LoadClassifier node = new LoadClassifier();


  }

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() {

    LoadClassifier node = new LoadClassifier();

    TestSink classifySink = new TestSink();
    node.connect(LoadClassifier.OPORT_OUT_DATA, classifySink);
  }

}

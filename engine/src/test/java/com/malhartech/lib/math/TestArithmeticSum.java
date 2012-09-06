/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.Sink;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestArithmeticSum {
    private static Logger LOG = LoggerFactory.getLogger(ArithmeticSum.class);

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

        ArithmeticSum node = new ArithmeticSum();

        // Insert tests for expected failure and success here
        node.myValidation(conf);
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() {
        
        ArithmeticSum node = new ArithmeticSum();

        TestSink sumSink = new TestSink();
        node.connect(ArithmeticSum.OPORT_SUM, sumSink);

        HashMap<String, Integer> input = new HashMap<String, Integer>();

        input.put("a", 2); input.put("b", 20); input.put("c", 1000);
        node.process(input); input.clear();

        input.put("a", 1);
        node.process(input); input.clear();

        input.put("a", 10); input.put("b", 5);
        node.process(input); input.clear();
        
        input.put("d", 55); input.put("b", 12);
        node.process(input); input.clear();
 
        input.put("d", 22);
        node.process(input); input.clear();
 
        input.put("d", 46); input.put("e", 2);
        node.process(input); input.clear();
  
        input.put("a", 23); input.put("d", 4);
        node.process(input); input.clear();
 
        input.put("d", 14);
        node.process(input); input.clear();

        node.endWindow(); // 
        Assert.assertEquals("number emitted tuples", 5, sumSink.collectedTuples.size()); // for keys "a", "b", "c", "d", "e"
        for (Object o : sumSink.collectedTuples) {
            HashMap<String, Integer> output = (HashMap<String, Integer>) o;
            for (Map.Entry<String, Integer> e : output.entrySet()) {
                if (e.getKey().equals("a")) {
                    Assert.assertEquals("emitted value for 'a' was ", new Double(36), e.getValue());
                } else if (e.getKey().equals("b")) {
                    Assert.assertEquals("emitted tuple for 'b' was ", new Double(37), e.getValue());
                } else if (e.getKey().equals("c")) {
                    Assert.assertEquals("emitted tuple for 'c' was ", new Double(1000), e.getValue());
                } else if (e.getKey().equals("d")) {
                    Assert.assertEquals("emitted tuple for 'd' was ", new Double(141), e.getValue());
                } else if (e.getKey().equals("e")) {
                    Assert.assertEquals("emitted tuple for 'e' was ", new Double(2), e.getValue());
                }
            }
        }
    }
}

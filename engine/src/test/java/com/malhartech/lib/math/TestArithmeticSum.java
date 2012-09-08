/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.Sink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Ignore
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

        HashMap<String, Double> input = new HashMap<String, Double>();

        input.put("a", 2.0); input.put("b", 20.0); input.put("c", 1000.0);
        node.process(input); input.clear();

        input.put("a", 1.0);
        node.process(input); input.clear();

        input.put("a", 10.0); input.put("b", 5.0);
        node.process(input); input.clear();

        input.put("d", 55.0); input.put("b", 12.0);
        node.process(input); input.clear();

        input.put("d", 22.0);
        node.process(input); input.clear();

        input.put("d", 14.2);
        node.process(input); input.clear();

        // Mix integers and doubles
        HashMap<String, Integer> inputi = new HashMap<String, Integer>();

        inputi.put("d", 46); inputi.put("e", 2);
        node.process(inputi); inputi.clear();

        inputi.put("a", 23); inputi.put("d", 4);
        node.process(inputi); inputi.clear();



        node.endWindow(); //
        // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
        Assert.assertEquals("number emitted tuples", 1, sumSink.collectedTuples.size());
        for (Object o : sumSink.collectedTuples) {
            HashMap<String, Number> output = (HashMap<String, Number>) o;
            for (Map.Entry<String, Number> e : output.entrySet()) {
                if (e.getKey().equals("a")) {
                    Assert.assertEquals("emitted value for 'a' was ", new Double(36), e.getValue().doubleValue());
                } else if (e.getKey().equals("b")) {
                    Assert.assertEquals("emitted tuple for 'b' was ", new Double(37), e.getValue().doubleValue());
                } else if (e.getKey().equals("c")) {
                    Assert.assertEquals("emitted tuple for 'c' was ", new Double(1000), e.getValue().doubleValue());
                } else if (e.getKey().equals("d")) {
                    Assert.assertEquals("emitted tuple for 'd' was ", new Double(141.2), e.getValue().doubleValue());
                } else if (e.getKey().equals("e")) {
                    Assert.assertEquals("emitted tuple for 'e' was ", new Double(2), e.getValue().doubleValue());
                }
            }
        }
    }
}

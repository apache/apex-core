/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.esotericsoftware.minlog.Log;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import java.util.Map;
import junit.framework.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestLoadClassifier {

    private static Logger LOG = LoggerFactory.getLogger(LoadClassifier.class);

    class TestSink implements Sink {

        HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();
        HashMap<String, Double> collectedTupleValues = new HashMap<String, Double>();

        /**
         * 
         * @param payload 
         */
        @Override
        public void process(Object payload) {
            if (payload instanceof Tuple) {
                // LOG.debug(payload.toString());
            } else {
                HashMap<String, Double> tuple = (HashMap<String, Double>) payload;
                for (Map.Entry<String, Double> e : tuple.entrySet()) {
                    Integer ival = collectedTuples.get(e.getKey());
                    if (ival == null) {
                        ival = new Integer(1);
                    } else {
                        ival = ival + 1;
                    }
                    collectedTuples.put(e.getKey(), ival);
                    collectedTupleValues.put(e.getKey(), e.getValue());
                }
            }
        }
        /**
         * 
         */
        public void clear() {
            collectedTuples.clear();
            collectedTupleValues.clear();
        }
    }

    /**
     * Test configuration and parameter validation of the node
     */
    @Test
    public void testNodeValidation() {

        NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());
        LoadClassifier node = new LoadClassifier();
        // String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        // String[] vstr = config.getTrimmedStrings(KEY_VALUES);


        conf.set(LoadClassifier.KEY_KEYS, "");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadClassifier.KEY_KEYS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadClassifier.KEY_KEYS,
                    e.getMessage().contains("is empty"));
        }

        conf.set(LoadClassifier.KEY_KEYS, "a,b,c"); // from now on keys are a,b,c
        conf.set(LoadClassifier.KEY_VALUEOPERATION, "blah");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadClassifier.KEY_VALUEOPERATION);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadClassifier.KEY_VALUEOPERATION,
                    e.getMessage().contains("not supported. Supported values are"));
        }
        conf.set(LoadClassifier.KEY_VALUEOPERATION, "replace"); // from now on valueoperation is "replace"

        conf.set(LoadClassifier.KEY_VALUES, "1,2,3,4");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadClassifier.KEY_VALUES);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadClassifier.KEY_VALUES,
                    e.getMessage().contains("does not match number of keys"));
        }

        conf.set(LoadClassifier.KEY_VALUES, "1,2a,3");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadClassifier.KEY_VALUES);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadClassifier.KEY_VALUES,
                    e.getMessage().contains("Value string should be float"));
        }

        conf.set(LoadClassifier.KEY_VALUES, "1,2,3");

        conf.set(LoadClassifier.KEY_WEIGHTS, "ia:60,10,35;;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("One of the keys in"));
        }

        conf.set(LoadClassifier.KEY_WEIGHTS, "ia:60,10,35;ib,10,75,15;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("need two strings separated by"));
        }

        conf.set(LoadClassifier.KEY_WEIGHTS, "ia:60,10,35;ib:10,75;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("does not match the number of keys"));
        }

        conf.set(LoadClassifier.KEY_WEIGHTS, "ia:60,10,35;ib:10,75,1a5;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("Weight string should be an integer"));
        }
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() {

        LoadClassifier node = new LoadClassifier();

        TestSink classifySink = new TestSink();
        node.connect(LoadClassifier.OPORT_OUT_DATA, classifySink);
        HashMap<String, Double> input = new HashMap<String, Double>();

        NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());

        conf.set(LoadClassifier.KEY_KEYS, "a,b,c");
        conf.set(LoadClassifier.KEY_VALUES, "1,4,5");
        conf.set(LoadGenerator.KEY_WEIGHTS, "ia:60,10,35;ib:10,75,15;ic:20,10,70;id:50,15,35");
        conf.set(LoadClassifier.KEY_VALUEOPERATION, "replace");

        conf.setInt("SpinMillis", 10);
        conf.setInt("BufferCapacity", 1024 * 1024);
        node.setup(conf);

        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("ia", 2.0);
            input.put("ib", 20.0);
            input.put("ic", 1000.0);
            input.put("id", 1000.0);
            node.process(input);
        }
        node.endWindow();
        int ival = 0;
        for (Map.Entry<String, Integer> e : classifySink.collectedTuples.entrySet()) {
            ival += e.getValue().intValue();
        }
        LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                ival,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
        }

        // Now test a node with no weights
        LoadClassifier nwnode = new LoadClassifier();
        classifySink.clear();
        nwnode.connect(LoadClassifier.OPORT_OUT_DATA, classifySink);
        conf.set(LoadGenerator.KEY_WEIGHTS, "");
        nwnode.setup(conf);

        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("ia", 2.0);
            input.put("ib", 20.0);
            input.put("ic", 1000.0);
            input.put("id", 1000.0);
            nwnode.process(input);
        }
        nwnode.endWindow();
        ival = 0;
        for (Map.Entry<String, Integer> e : classifySink.collectedTuples.entrySet()) {
            ival += e.getValue().intValue();
        }
        LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                ival,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
        }

        
        // Now test a node with no weights and no values
        LoadClassifier nvnode = new LoadClassifier();
        classifySink.clear();
        nvnode.connect(LoadClassifier.OPORT_OUT_DATA, classifySink);
        conf.set(LoadGenerator.KEY_WEIGHTS, "");
        conf.set(LoadClassifier.KEY_VALUES, "");
        nvnode.setup(conf);    
        
        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("ia", 2.0);
            input.put("ib", 20.0);
            input.put("ic", 500.0);
            input.put("id", 1000.0);
            nvnode.process(input);
        }
        nvnode.endWindow();
        ival = 0;
        for (Map.Entry<String, Integer> e : classifySink.collectedTuples.entrySet()) {
            ival += e.getValue().intValue();
        }
        LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                ival,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f",
                    ieval.intValue(),
                    ve.getKey(),
                    ve.getValue()));
        }
    }
}

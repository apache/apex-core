/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.dag.Component;
import com.malhartech.dag.DefaultSerDe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stram.ManualScheduledExecutorService;
import com.malhartech.stram.WindowGenerator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestLoadGenerator {

    private static Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);

    class TestSink implements Sink {
        
        HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();

        //DefaultSerDe serde = new DefaultSerDe();
        int count = 0;
        boolean test_hashmap = false;

        
        @Override
        public void process(Object payload) {
            if (payload instanceof Tuple) {
                // LOG.debug(payload.toString());
            }
            else {
                if (test_hashmap) {
                    HashMap<String, Double> tuple = (HashMap<String, Double>) payload;
                    for (Map.Entry<String, Double> e : tuple.entrySet()) {
                        String str = e.getKey();
                        Integer val = collectedTuples.get(str);
                        if (val != null) {
                            val = val + 1;
                        } else {
                            val = new Integer(1);
                        }
                        collectedTuples.put(str, val);
                    }
                }
                else {
                    String str = (String) payload;
                    Integer val = collectedTuples.get(str);
                    if (val != null) {
                        val = val + 1;
                    } else {
                        val = new Integer(1);
                    }
                    collectedTuples.put(str, val);
                }
                count++;
                //serde.toByteArray(payload);
             }
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
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadGenerator.KEY_KEYS,
                    e.getMessage().contains("is empty"));
        }

        conf.set(LoadGenerator.KEY_KEYS, "a,b,c,d"); // from now on keys would be a,b,c,d
        conf.set(LoadGenerator.KEY_WEIGHTS, "10.4,40,20,30");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadGenerator.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadGenerator.KEY_WEIGHTS,
                    e.getMessage().contains("should be an integer"));
        }

        conf.set(LoadGenerator.KEY_WEIGHTS, "10,40,20"); // from now on weights would be 10,40,20,30
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadGenerator.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadGenerator.KEY_WEIGHTS,
                    e.getMessage().contains("does not match number of keys"));
        }

        conf.set(LoadGenerator.KEY_WEIGHTS, ""); // from now on weights would be 10,40,20,30
        conf.set(LoadGenerator.KEY_VALUES, "a,2,3,4");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadGenerator.KEY_VALUES);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadGenerator.KEY_VALUES,
                    e.getMessage().contains("should be float"));
        }

        conf.set(LoadGenerator.KEY_WEIGHTS, "10,40,30,20"); // from now on weights would be 10,40,20,30
        conf.set(LoadGenerator.KEY_VALUES, "1,2,3");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadGenerator.KEY_VALUES);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadGenerator.KEY_VALUES,
                    e.getMessage().contains("does not match number of keys"));
        }

        conf.set(LoadGenerator.KEY_VALUES, "1,2,3,4");
        conf.set(LoadGenerator.KEY_TUPLES_PER_SEC, "0");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadGenerator.KEY_VALUES);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadGenerator.KEY_VALUES + " and " + LoadGenerator.KEY_TUPLES_PER_SEC,
                    e.getMessage().contains("has to be > 0"));
        }

        conf.set(LoadGenerator.KEY_VALUES, "1,2,3,4");
        conf.set(LoadGenerator.KEY_STRING_SCHEMA, "true");
        conf.set(LoadGenerator.KEY_TUPLES_PER_SEC, "1");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + LoadGenerator.KEY_STRING_SCHEMA);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + LoadGenerator.KEY_STRING_SCHEMA + " and " + LoadGenerator.KEY_VALUES,
                    e.getMessage().contains("if string_schema"));
        }               
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() {

        final LoadGenerator node = new LoadGenerator();
        final ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
        final WindowGenerator wingen = new WindowGenerator(mses);

        Configuration config = new Configuration();
        config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0);
        config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);
        wingen.setup(config);

        Sink input = node.connect(Component.INPUT, wingen);
        wingen.connect("mytestnode", input);

        TestSink lgenSink = new TestSink();
        node.connect(LoadGenerator.OPORT_DATA, lgenSink);
        NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());

        conf.set(LoadGenerator.KEY_KEYS, "a,b,c,d");
//        conf.set(LoadGenerator.KEY_VALUES, "1,2,3,4");
        conf.set(LoadGenerator.KEY_VALUES, "");
        conf.set(LoadGenerator.KEY_STRING_SCHEMA, "true");
        conf.set(LoadGenerator.KEY_WEIGHTS, "10,40,20,30");
        conf.setInt(LoadGenerator.KEY_TUPLES_PER_SEC, 10000000);
        conf.setInt("SpinMillis", 10);
        conf.setInt("BufferCapacity", 1024 * 1024);
        
        node.setup(conf);

        final AtomicBoolean inactive = new AtomicBoolean(true);
        new Thread() {
            @Override
            public void run() {
                inactive.set(false);
                node.activate(new NodeContext("LoadGeneratorTestNode"));
            }
        }.start();

        /**
         * spin while the node gets activated.
         */
        try {
            do {
                Thread.sleep(20);
            } while (inactive.get());
        } catch (InterruptedException ex) {
            LOG.debug(ex.getLocalizedMessage());
        }
        wingen.activate(null);
        for (int i = 0; i < 250; i++) {
            mses.tick(1);
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                LOG.error("Unexpected error while sleeping for 1 s", e);
            }
        }
        // Verify that the probability worked
        node.deactivate();
        // Assert.assertEquals("number emitted tuples", 5000, lgenSink.collectedTuples.size());
//        LOG.debug("Processed {} tuples out of {}", lgenSink.collectedTuples.size(), lgenSink.count);
        LOG.debug("Processed {} tuples", lgenSink.count);
        for (Map.Entry<String, Integer> e: lgenSink.collectedTuples.entrySet()) {
            LOG.debug("{} tuples for key {}", e.getValue().intValue(), e.getKey());
        }
    }
}

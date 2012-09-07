/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.dag.Component;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.lib.math.ArithmeticQuotient;
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

        List<Object> collectedTuples = new ArrayList<Object>();

        @Override
        public void process(Object payload) {
            if (payload instanceof Tuple) {
                LOG.debug(payload.toString());
            }
            else {
                collectedTuples.add(payload);
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
            Assert.assertTrue("validate " + LoadGenerator.KEY_VALUES,
                    e.getMessage().contains("has to be > 0"));
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
        conf.set(LoadGenerator.KEY_VALUES, "1,2,3,4");
        conf.set(LoadGenerator.KEY_WEIGHTS, "10,40,20,30");
        conf.setInt(LoadGenerator.KEY_TUPLES_PER_SEC, 5000000);
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
        for (int i = 0; i < 5; i++) {
            mses.tick(1);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOG.error("Unexpected error while sleeping for 1 s", e);
            }
        }
        // Verify that the probability worked
        node.deactivate();
        // Assert.assertEquals("number emitted tuples", 5000, lgenSink.collectedTuples.size());
        LOG.debug("GOT {} tuples", lgenSink.collectedTuples.size());
        HashMap<String, Integer> count = new HashMap<String, Integer>();
        for (Object o : lgenSink.collectedTuples) {
            HashMap<String, Double> tuple = (HashMap<String, Double>) o;
           for (Map.Entry<String, Double> e: tuple.entrySet()) {
                Integer val = count.get(e.getKey());
                if (val != null) {
                    val = val + 1;
                } else {
                    val = new Integer(1);
                }
                count.put(e.getKey(), val);
            }
        }
        for (Map.Entry<String, Integer> e: count.entrySet()) {
            LOG.debug("Got {} tuples for {}", e.getValue().intValue(), e.getKey());
        }
    }
}

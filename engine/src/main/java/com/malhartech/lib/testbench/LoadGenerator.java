/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.AbstractInputNode;
import com.malhartech.dag.EndStreamTuple;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import com.malhartech.lib.math.ArithmeticSum;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 *
 * @author amol
 */
@NodeAnnotation(
        ports = {
    @PortAnnotation(name = LoadGenerator.OPORT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class LoadGenerator extends AbstractInputNode {

    public static final String OPORT_DATA = "data";
    private static Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);
    boolean hasvalues = false;
    boolean hasweights = false;
    int tuples_per_ms = 1;
    HashMap<String, Double> keys = new HashMap<String, Double>();
    HashMap<Integer, String> wtostr_index = new HashMap<Integer, String>();

    ArrayList<Integer> weights = new ArrayList<Integer>();
    int total_weight = 0;
    int num_keys = 0;
    private Random random = new Random();
    private volatile boolean shutdown = false;
    private boolean outputConnected = false;
    /**
     * keys are comma seperated list of keys for the load. These keys are send
     * one per tuple as per the other parameters
     *
     */
    public static final String KEY_KEYS = "keys";
    /**
     * values are to be assigned to each key. The tuple thus is a key,value
     * pair. The value field can either be empty (no value to any key), or a
     * comma separated list of values. If values list is provided, the number
     * must match the number of keys
     */
    public static final String KEY_VALUES = "values";
    /**
     * The weights define the probability of each key being assigned to current
     * tuple. The total of all weights is equal to 100%. If weights are not
     * specified then the probability is equal.
     */
    public static final String KEY_WEIGHTS = "weights";
    /**
     * The number of tuples sent out per milli second
     */
    public static final String KEY_TUPLES_PER_MS = "tuples_per_ms";

    @Override
    public void endWindow() {;
    }

    @Override
    public void beginWindow() {;
    }

    public boolean myValidation(NodeConfiguration config) {
        String[] wstr = config.getTrimmedStrings(KEY_WEIGHTS);
        String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        String[] vstr = config.getTrimmedStrings(KEY_VALUES);
        boolean ret = true;

        if (kstr == null) {
            ret = false;
            throw new IllegalArgumentException("Parameter \"key\" is empty");
        } else {
            LOG.info(String.format("Number of keys are %d", kstr.length));
        }
        if (wstr == null) {
            LOG.info("weights was not provided, so keys would be equally weighted");
        } else {
            for (String s : wstr) {
                try {
                    Integer.parseInt(s);
                } catch (NumberFormatException e) {
                    ret = false;
                    throw new IllegalArgumentException(String.format("Weight string should be integer(%s)", s));
                }   
            }
        }
        if (vstr == null) {
            LOG.info("values was not provided, so keys would have value of 0");
        } else {
            for (String s : vstr) {
                try {
                    Double.parseDouble(s);
                } catch (NumberFormatException e) {
                    ret = false;
                    throw new IllegalArgumentException(String.format("Value string should be float(%s)", s));
                }
            }
        }

        if ((wstr != null) && (wstr.length != kstr.length)) {
            ret = false;
            throw new IllegalArgumentException(
                    String.format("Number of weights (%d) does not match number of keys (%d)",
                    wstr.length, kstr.length));
        }
        if ((vstr != null) && (vstr.length != kstr.length)) {
            ret = false;
            throw new IllegalArgumentException(
                    String.format("Number of values (%d) does not match number of keys (%d)",
                    vstr.length, kstr.length));
        }

        tuples_per_ms = config.getInt(KEY_TUPLES_PER_MS, 1);
        if (tuples_per_ms <= 0) {
            ret = false;
            throw new IllegalArgumentException(
                    String.format("tuples_per_ms (%d) has to be > 0", tuples_per_ms));
        } else {
            LOG.info(String.format("Using %d tuples per millisecond", tuples_per_ms));
        }
        // Should enforce an upper limit
        return ret;
    }

    @Override
    public void setup(NodeConfiguration config) {
        super.setup(config);
        if (!myValidation(config)) {
            throw new IllegalArgumentException("Did not pass validation");
        }

        String[] wstr = config.getTrimmedStrings(KEY_WEIGHTS);
        String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        String[] vstr = config.getTrimmedStrings(KEY_VALUES);

        tuples_per_ms = config.getInt(KEY_TUPLES_PER_MS, 1);
        hasweights = (wstr != null);
        hasvalues = (vstr != null);
        // Keys and weights would are accessed via same key
        num_keys = kstr.length;

        int i = 0;
        for (String s : kstr) {
            if (hasweights) {
                weights.add(Integer.parseInt(wstr[i]));
                total_weight += Integer.parseInt(wstr[i]);
            } else {
                total_weight += 100;
            }
            if (hasvalues) {
                keys.put(s, new Double(Double.parseDouble(vstr[i])));
            } else {
                keys.put(s, new Double(0.0));
            }
            wtostr_index.put(i, s);
            i += 1;
        }
    }

    @Override
    public void connected(String id, Sink dagpart) {
        if (id.equals(OPORT_DATA)) {
            outputConnected = true;
        }
    }

    @Override
    public void deactivate() {
        shutdown = true;
        super.deactivate();
    }

    @Override
    public void activate(NodeContext context) {
        super.activate(context);
        int i = 0;

        while (!shutdown) {
            if (outputConnected) {
                // send tuples as per weights and then sleep for 1ms
                while (i < tuples_per_ms) {
                    int rval = random.nextInt(total_weight);
                    int j = 0;
                    int wval = 0;
                    for (Integer e : weights) {
                        wval += e.intValue();
                        if (wval >= rval) break;
                        j++;
                    }
                    // wval is the key index
                    HashMap<String, Double> tuple = new HashMap<String, Double>();
                    String key = wtostr_index.get(new Integer(j));
                    tuple.put(key, keys.get(key)); // the key
                    
                    emit(OPORT_DATA, tuple);
                    i++;
                }
                
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    LOG.error("Unexpected error while sleeping for 1 ms", e);
                }
            }
            try { // Wait till output port is connected in the deployment of dag
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("Unexpected error while generating tuples", e);
            }
        }
        LOG.info("Finished generating tuples");
    }
}

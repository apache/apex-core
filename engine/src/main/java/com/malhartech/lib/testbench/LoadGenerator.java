/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.lib.math.ArithmeticSum;
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
public class LoadGenerator extends AbstractNode {

    public static final String OPORT_DATA = "data";
    boolean hasvalues = false;
    boolean hasweights = false;
    int tuples_per_ms = 1;
    HashMap<String, String> keys = new HashMap<String, String>();
    HashMap<String, Integer> weights = new HashMap<String, Integer>();

    Integer total_weight = 0;
    int num_keys = 0;
    private Random random = new Random();
    
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
    public void setup(NodeConfiguration config) {
        super.setup(config);
        tuples_per_ms = config.getInt(KEY_TUPLES_PER_MS, 1);

        String[] wstr = config.getTrimmedStrings(KEY_WEIGHTS);
        String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        String[] vstr = config.getTrimmedStrings(KEY_VALUES);

        hasweights = (wstr != null);
        hasvalues = (vstr != null);
        int i = 0;
        // Keys and weights would are accessed via same key
        num_keys = kstr.length;
        for (String s : kstr) {
            if (hasweights) {
                weights.put(s, Integer.parseInt(wstr[i]));
                total_weight += Integer.parseInt(wstr[i]);
            } else {
                total_weight += 100;
            }
            if (hasvalues) {
                keys.put(s, vstr[i]);
            } else {
                keys.put(s, "");
            }
            i += 1;
        }
        //HashMap<String, Integer> weights = new HashMap<String, Integer>();
        //Integer total_weight = 1;
    }

    @Override
    public void process(Object payload) {
        // tbd
        int loc = random.nextInt(total_weight);
        // loc would provide the key to access
        // Need to store keys in a correct indexed fashion

    }

    @Override
    public void endWindow() {
        super.endWindow();
    }

    @Override
    public boolean checkConfiguration(NodeConfiguration config) {
        boolean ret = true;

        String[] wstr = config.getTrimmedStrings(KEY_WEIGHTS);
        String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        String[] vstr = config.getTrimmedStrings(KEY_VALUES);

        if (kstr == null) {
            ret = false;
        }
        if ((wstr != null) && (wstr.length != kstr.length)) {
            ret = false;
        }
        if ((vstr != null) && (vstr.length != kstr.length)) {
            ret = false;
        }

        tuples_per_ms = config.getInt(KEY_TUPLES_PER_MS, 1);
        if (tuples_per_ms <= 0) { // should also enforce an upper limit
            ret = false;
        }
        return ret && super.checkConfiguration(config);
    }
}

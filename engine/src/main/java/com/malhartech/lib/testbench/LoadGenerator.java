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
    
    int range = 0;
    boolean hasvalues = false;
    int tuples_per_ms = 1;


    HashMap<String, Number> keys = new HashMap<String, Number>();
    HashMap<String, Integer> weights = new HashMap<String, Integer>();
    Integer total_weight = 1;
    
    /**
     * keys are comma seperated list of keys for the load. These keys are send one per tuple as per the other parameters
     * 
     */
    public static final String KEY_KEYS = "keys";

    /**
     * values are to be assigned to each key. The tuple thus is a key,value pair. The value field can either be empty (no value to 
     * any key), or a comma separated list of values. If values list is provided, the number must match the number of keys
     */
    public static final String KEY_VALUES = "values";
    
    /**
     * The weights define the probability of each key being assigned to current tuple. The total of all weights is equal to 100%.
     * If weights are not specified then the probability is equal.
     */
    public static final String KEY_WEIGHTS = "weights";

    /**
     * For each window the the weights are changed randomly within range (+/- range). This allows some randomization of the load
     */
    public static final String KEY_RANGE = "range";
    
    /**
     * The number of tuples sent out per milli second
     */
    
    public static final String KEY_TUPLES_PER_MS = "tuples_per_ms";
    

    
    @Override
    public void setup(NodeConfiguration config) {
        
        
        
        super.setup(config);
    }

    
    
    @Override
    public void process(Object payload) {
        // tbd
    }
    
    @Override
    public void beginWindow() {
        super.beginWindow();
    }

    @Override
    public void endWindow() {
        super.endWindow();
    }

    
    @Override
    public boolean checkConfiguration(NodeConfiguration config) {
        boolean ret = true;
        
        return ret && super.checkConfiguration(config);
    }
}


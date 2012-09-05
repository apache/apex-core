/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

/**
 *
 * Takes in two streams via input ports "numerator" and "denominator".
 * At the end of window computes the quotient for each key and emits the result<p>
 * <br>
 * 
 * @author amol
 */
import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;

@NodeAnnotation(
        ports = {
    @PortAnnotation(name = "numerator", type = PortType.INPUT),
    @PortAnnotation(name = "denominator", type = PortType.INPUT),
    @PortAnnotation(name = "quotient", type = PortType.OUTPUT)
})
public class ArithmeticQuotient extends AbstractNode {
    long mult_by = 1;
    boolean comp_margin = false;
    boolean integermath = false;
   
    /**
     * Multiplies the quotient by this number. Ease of use for percentage (* 100) or CPM (* 1000)
     */
    public static final String KEY_MULTIPLY_BY = "multiply_by";
    
    /**
     * Computes margin instead of quotient. 
     */
    public static final String KEY_COMPUTE_MARGIN = "compute_margin";
    
    /**
     * 
     */
    public static final String KEY_INTEGERMATH = "integermath";
    
    /**
     * 
     * @param config 
     */
    @Override
    public void setup(NodeConfiguration config) {
        super.setup(config);
        mult_by = config.getLong(KEY_MULTIPLY_BY, 1);
        comp_margin = config.getBoolean(KEY_COMPUTE_MARGIN, false);
        integermath = config.getBoolean(KEY_INTEGERMATH, false);
    }
    
    @Override
    public void process(Object payload) {
        // TBD
    }

    @Override
    public boolean checkConfiguration(NodeConfiguration config) {
        boolean ret = true;
        // Check each value
        // In v0.2 most of common checks should be done via annotations
        
        return ret && super.checkConfiguration(config);
    }
    
}

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in two streams via input ports "numerator" and "denominator". At the
 * end of window computes the quotient for each key and emits the result on port
 * "quotient".<p>
 * <br>
 * Each stream is added to a hash. The values are added for each key within the window and for each stream.<br>
 * If compute_margin is true then the result is 1 - numerator/denominator expressed as a percentage. Ideally
 * multiply_by should be 1 in this case.<br>
 * This node only functions in a windowed stram application<br>
 * Compile time error processing is done on configuration parameters<br>
 * property "compute_margin" has to be boolean ("true" or "false").<br>
 * property "multiply_by" has to be an integer.<br>
 * input ports "numerator", "denominator" must be connected.<br>
 * one of the out bound ports "quotient" or "_error" must be connected.<br>
 * "windowed" has to be true<br>
 * Run time error processing are emitted on _error port. The errors are:<br>
 * Divide by zero (Error): no result is emitted on "outport".<br>
 * Input tuple not an integer on denominator stream: This tuple would not be counted towards the result.<br> 
 * Input tuple not an integer on numerator stream: This tuple would not be counted towards the result.<br>
 * <br>
 *
 * @author amol<br>
 *
 */
@NodeAnnotation(
        ports = {
    @PortAnnotation(name = ArithmeticQuotient.IPORT_NUMERATOR, type = PortType.INPUT),
    @PortAnnotation(name = ArithmeticQuotient.IPORT_DENOMINATOR, type = PortType.INPUT),
    @PortAnnotation(name = ArithmeticQuotient.OPORT_QUOTIENT, type = PortType.OUTPUT)
})
public class ArithmeticQuotient extends AbstractNode {

    long mult_by = 1;
    boolean comp_margin = false;
    public static final String IPORT_NUMERATOR = "numerator";
    public static final String IPORT_DENOMINATOR = "denominator";
    public static final String OPORT_QUOTIENT = "quotient";
    HashMap<String, Number> numerators = new HashMap<String, Number>();
    HashMap<String, Number> denominators = new HashMap<String, Number>();
    HashMap<String, Number> in_tuple = new HashMap<String, Number>();
    /**
     * Multiplies the quotient by this number. Ease of use for percentage
     * (* 100) or CPM (* 1000)
     * 
     */
    public static final String KEY_MULTIPLY_BY = "multiply_by";
    /**
     * Computes margin instead of quotient.
     */
    public static final String KEY_COMPUTE_MARGIN = "compute_margin";

    /**
     *
     * @param config
     */
    @Override
    public void setup(NodeConfiguration config) {
        super.setup(config);
        mult_by = config.getLong(KEY_MULTIPLY_BY, 1);
        comp_margin = config.getBoolean(KEY_COMPUTE_MARGIN, false);
    }

    @Override
    public void process(Object payload) {
        in_tuple = (HashMap<String, Number>) payload;
        Number val = null;
        Boolean edata = false;
        for (Map.Entry<String, Number> e : in_tuple.entrySet()) {
            String iport = getActivePort();
            if (IPORT_NUMERATOR == iport) {
                val = numerators.get(e.getKey());
            } else if (IPORT_DENOMINATOR == iport) {
                val = denominators.get(e.getKey());
            } else {
                edata = true;
            }
            if (!edata) {
                if (val == null) {
                    val = e.getValue();
                } else {
                    val = new Double(val.doubleValue() + e.getValue().doubleValue());
                }
                if (IPORT_NUMERATOR == iport) {
                    numerators.put(e.getKey(), val);
                } else if (IPORT_DENOMINATOR == iport) { // just else would do
                    denominators.put(e.getKey(), val);
                }
            } else {
                // emit error data on port _error
            }
        }
    }

    @Override
    public void endWindow() {
        Number dval = null;
        Number nval = null;
        for (Map.Entry<String, Number> e : denominators.entrySet()) {
            dval = e.getValue ();
            nval = numerators.get(e.getKey());
            if (nval == null) {
                // emit, key, 0.0
            }
            else {
                numerators.remove(e.getKey());
                // email key, nval/dval * multiply_by; if compute margin, emit data in margin
            }         
        }  
        // Now if numerators has any keys issue divide by zero error
        for (Map.Entry<String, Number> e : numerators.entrySet()) {
            // emit error
        }
        
        numerators.clear();
        denominators.clear();
        super.endWindow();
    }


    @Override
    public boolean checkConfiguration(NodeConfiguration config) {
        boolean ret = true;
        // Check each value for its range
        // compute_margin has to be true or false
        // multiply_by has to be an integer
        // windowed has to be true
        // 
        // In v0.2 most of common checks should be done via annotations

        return ret && super.checkConfiguration(config);
    }
}

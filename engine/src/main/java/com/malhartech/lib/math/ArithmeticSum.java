/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". At end of window sums all values
 * for each key and emits them on port "sum"<p>
 * <br> Values are stored in a hash<br>
 * This node only functions in a windowed stram application <br>
 * Compile time error processing is done on configuration parameters<br>
 * property "distinct" is false by default. If true the values are ignored and
 * it performs like a Set<br> 
 * input port "data" must be connected<br>
 * output port "sum" must be connected<br>
 * "windowed" has to be true<br> 
 * Run time error processing are emitted on _error port. The errors are:<br>
 * Value is not a Number<br>
 *
 * @author amol
 */
@NodeAnnotation(
        ports = {
    @PortAnnotation(name = ArithmeticSum.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
    @PortAnnotation(name = ArithmeticSum.OPORT_SUM, type = PortAnnotation.PortType.OUTPUT)
})
public class ArithmeticSum extends AbstractNode {

    public static final String IPORT_DATA = "data";
    public static final String OPORT_SUM = "sum";
    HashMap<String, Number> sum = new HashMap<String, Number>();
    HashMap<String, Number> in_tuple = new HashMap<String, Number>();

    /**
     * Process each tuple
     * @param payload 
     */
    @Override
    public void process(Object payload) {
        in_tuple = (HashMap<String, Number>) payload;
        Number val = null;
        for (Map.Entry<String, Number> e : in_tuple.entrySet()) {
            val = sum.get(e.getKey());
            if (val != null) {
                val = new Double(val.doubleValue() + e.getValue().doubleValue());
            } else {
                val = new Double(0.0);
            }
            sum.put(e.getKey(), val);
        }
    }
    
    /**
     * Node only works in windowed mode. Emits all data upon end of window tuple
     */
    @Override
    public void endWindow() {
        for (Map.Entry<String, Number> e : sum.entrySet()) {
            // emit e.getKey(), and e.getValue() as a tuple            
        }
        sum.clear();
        super.endWindow();
    }

    /**
     * 
     * Checks for user specific configuration values<p>
     * @param config
     * @return boolean
     */
    @Override
    public boolean checkConfiguration(NodeConfiguration config) {
        boolean ret = true;
        // TBD
        return ret && super.checkConfiguration(config);
    }
}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in two streams via input ports "numerator" and "denominator". At the
 * end of window computes the margin for each key and emits the result on port
 * "margin" (1 - numerator/denominator).<p> <br> Each stream is added to a hash.
 * The values are added for each key within the window and for each stream.<<br>
 * This node only functions in a windowed stram application<br> <br> Compile
 * time error processing is done on configuration parameters<br> input ports
 * <b>numerator</b>, <b>denominator</b> must be connected.<br> outbound port
 * <b>margin</b> must be connected.<br> <br><b>All run time errors are TBD</b>
 * <br> Run time error processing are emitted on _error port. The errors
 * are:<br> Divide by zero (Error): no result is emitted on "outport".<br> Input
 * tuple not an integer on denominator stream: This tuple would not be counted
 * towards the result.<br> Input tuple not an integer on numerator stream: This
 * tuple would not be counted towards the result.<br> <br>
 *
 * @author amol<br>
 *
 */
@NodeAnnotation(
        ports = {
    @PortAnnotation(name = ArithmeticMargin.IPORT_NUMERATOR, type = PortType.INPUT),
    @PortAnnotation(name = ArithmeticMargin.IPORT_DENOMINATOR, type = PortType.INPUT),
    @PortAnnotation(name = ArithmeticMargin.OPORT_MARGIN, type = PortType.OUTPUT)
})
public class ArithmeticMargin extends AbstractNode {

    public static final String IPORT_NUMERATOR = "numerator";
    public static final String IPORT_DENOMINATOR = "denominator";
    public static final String OPORT_MARGIN = "margin";
    private static Logger LOG = LoggerFactory.getLogger(ArithmeticMargin.class);
    HashMap<String, Number> numerators = new HashMap<String, Number>();
    HashMap<String, Number> denominators = new HashMap<String, Number>();

    /**
     *
     * @param config
     */
    @Override
    public void process(Object payload) {
        Map<String, Number> active;
        if (IPORT_NUMERATOR.equals(getActivePort())) {
            active = numerators;
        } else {
            active = denominators;
        }

        for (Map.Entry<String, Number> e : ((HashMap<String, Number>) payload).entrySet()) {
            Number val = active.get(e.getKey());
            if (val == null) {
                val = e.getValue();
            } else {
                val = new Double(val.doubleValue() + e.getValue().doubleValue());
            }
            active.put(e.getKey(), val);
        }
    }

    @Override
    public void endWindow() {
        HashMap<String, Number> tuples = new HashMap<String, Number>();
        for (Map.Entry<String, Number> e : denominators.entrySet()) {
            Number nval = numerators.get(e.getKey());
            if (nval == null) {
                nval = new Double(0.0);
            } else {
                numerators.remove(e.getKey()); // so that all left over keys can be reported
            }
            tuples.put(e.getKey(), new Double(1 - nval.doubleValue() / e.getValue().doubleValue()));
        }
        emit(tuples);
        /* Now if numerators has any keys issue divide by zero error
         for (Map.Entry<String, Number> e : numerators.entrySet()) {
         // emit error
         }
         */
        numerators.clear();
        denominators.clear();
        super.endWindow();
    }
}

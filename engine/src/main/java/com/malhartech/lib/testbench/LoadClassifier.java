/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractNode;

/**
 *
 * @author amol
 */
@NodeAnnotation(
        ports = {
    @PortAnnotation(name = LoadClassifier.IPORT_IN_DATA, type = PortAnnotation.PortType.INPUT),
    @PortAnnotation(name = LoadClassifier.OPORT_OUT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class LoadClassifier extends AbstractNode {
    public static final String IPORT_IN_DATA = "in_data";
    public static final String OPORT_OUT_DATA = "out_data";

/*  
    # stram.template.classifier.keys= #Must specify
# stram.template.classifer.weights= #Default is same weights for all keys
# stram.template.classifier.delimiter= #Default is ','
# stram.template.classifier.values= #Default is no values; If values are provided they should be as many as keys
# stram.template.classifier.valueoperation= #default is replace; other supported are add, mult, div, append
*/
    /**
     * keys are comma seperated list of keys to append to keys in in_data stream<p>
     * The out bound keys are in_data(key)<delimiter>key
     *
     */
    public static final String KEY_KEYS = "keys";
    /**
     * values are to be assigned to each key. The tuple thus is a newkey,newvalue
     * pair. The value field can either be empty in which case the value in in_data tuple is 
     * passed through as is; or a comma separated list of values. These values are then operated
     * upon the incoming values (see valueoperation). If values list is provided,
     * the number must match the number of keys
     * 
     */
    public static final String KEY_VALUES = "values";
    /**
     * The weights define the probability of each key being assigned to current
     * in_data tuple based on the in_data tuple key. The total of all weights is equal to 100%.
     * If weights are not specified then the append probability is equal.
     */
    public static final String KEY_WEIGHTS = "weights";
    
    /**
     * operation to be done between the incoming values and inserted values by this node. The supported operations are
     * replace: Is the default operation and would simply ignore the incoming value and insert new one.
     * add: Adds to the incoming value
     * mult: Multiplies the incoming value
     * append: Appends to the incoming value. The same delimiter is used as that of the key
     * 
     */
    public static final String KEY_VALUEOPERATION = "valueoperation";
  
    
    @Override
    public void process(Object payload) {
        // TBD  
    }
    
}

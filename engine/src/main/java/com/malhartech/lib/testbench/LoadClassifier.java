/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.Sink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);

    HashMap<String, Double> keys = new HashMap<String, Double>();
    HashMap<Integer, String> wtostr_index = new HashMap<Integer, String>();
    
    // One of inkeys (Key to weight hash) or noweight (even weight) would be not null
    HashMap<String, ArrayList<Integer>> inkeys = null;
    ArrayList<Integer> noweight = null;
    boolean hasvalues = false;

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
    int total_weight = 0;
    private Random random = new Random();
    
    enum value_operation {VOPR_REPLACE,VOPR_ADD,VOPR_MULT,VOPR_APPEND} ;
    value_operation voper;
    
    private volatile boolean shutdown = false;
    private boolean outputConnected = false;
    
   /**
     * Not used, but overridden as it is abstract
     */
    @Override
    public void endWindow() {
    }

    /**
     * Not used, but overridden as it is abstract
     */
    @Override
    public void beginWindow() {
    }

      /**
     * 
     * Code to be moved to a proper base method name
     * @param config
     * @return boolean
     */
    public boolean myValidation(NodeConfiguration config) {
        
        boolean ret = true;

        String iwstr = config.get(KEY_WEIGHTS, "");
        String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        String[] vstr = config.getTrimmedStrings(KEY_VALUES);
        String vostr = config.get(KEY_VALUEOPERATION, "");

        
        if (kstr.length == 0) {
            ret = false;
            throw new IllegalArgumentException("Parameter \"key\" is empty");
        } else {
            LOG.info(String.format("Number of keys are %d", kstr.length));
        }
        
        if (!iwstr.isEmpty()) { // if empty noweights would be used
            String[] wstr = iwstr.split(";");
            for (String s : wstr) { // Each wstr is in_key:val1,val2,valN where N = num of keys
                if (s.isEmpty()) {
                    ret = false;
                    throw new IllegalArgumentException("One of the keys in \"weights\" is empty");
                } else {
                    String[] keywstrs = s.split(":");
                    if (keywstrs.length != 2) {
                        ret = false;
                        throw new IllegalArgumentException(
                                String.format("Property \"weights\" has a bad key \"%s\" (need two strings separated by ':')", s));
                    }
                    String[] kwstrs = keywstrs[1].split(","); // Keywstrs[0] is the in_key
                    if (kwstrs.length != kstr.length) {
                        ret = false;
                        throw new IllegalArgumentException(
                                String.format("Number of weights (%d) in \"%s\" does not match the number of keys (%d) in \"%s\"",
                                kwstrs.length, keywstrs[1], kstr.length, config.get(KEY_KEYS, "")));
                    }
                    else { // Now you get weights for each key
                        for (String ws : kwstrs) {
                            try {
                                Integer.parseInt(ws);
                            } catch (NumberFormatException e) {
                                ret = false;
                                throw new IllegalArgumentException(String.format("Weight string should be an integer(%s)", ws));
                            }
                        }
                    }
                }
            }
        }

        hasvalues = (vstr.length != 0);
        if (!hasvalues) {
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

        if (hasvalues && (vstr.length != kstr.length)) {
            ret = false;
            throw new IllegalArgumentException(
                    String.format("Number of values (%d) does not match number of keys (%d)",
                    vstr.length, kstr.length));
        }
          
        if (vostr.isEmpty() || vostr.equals("replace")) {
            voper = value_operation.VOPR_REPLACE; // Default is replace
        }
        else if (vostr.equals("add")) {
            voper = value_operation.VOPR_ADD;            
        }
        else if (vostr.equals("mult")) {
            voper = value_operation.VOPR_MULT;            
        }
        else if (vostr.equals("append")) {
            voper = value_operation.VOPR_APPEND;
        }
        else {
            ret = false;
            throw new IllegalArgumentException(
                    String.format("Value opertion (%s) not supported. Supported values are \"replace\",\"add\",\"mult\",\"append\"", vostr));
        } 
        return ret;
    }
    
   /**
     * Sets up all the config parameters. Assumes checking is done and has passed
     * @param config 
     */
    @Override
    public void setup(NodeConfiguration config) {
        super.setup(config);
        if (!myValidation(config)) {
            throw new IllegalArgumentException("Did not pass validation");
        }

        // example format for iwstr is "home:60,10,35;finance:10,75,15;sports:20,10,70;mail:50,15,35"
        String iwstr = config.get(KEY_WEIGHTS, "");
        String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        String[] vstr = config.getTrimmedStrings(KEY_VALUES);

        String[] wstr = null;
        if (!iwstr.isEmpty()) {
            wstr = iwstr.split(";");
            inkeys = new HashMap<String, ArrayList<Integer>>();
            for (String ts : wstr) { // ts is top string as <key>:weight1,weight2,...
                String[] twostr = ts.split(":");
                String[] weights = twostr[1].split(",");
                ArrayList<Integer> alist = new ArrayList<Integer>();
                Integer wtotal = 0;
                for (String ws : weights) {
                    alist.add(Integer.parseInt(ws));
                    wtotal += Integer.parseInt(ws);
                }
                alist.add(wtotal);
                inkeys.put(twostr[0], alist);
            }
        } else {
            // noweight would be used for all in_keys
            noweight = new ArrayList<Integer>();
            for (String s : kstr) {
                noweight.add(100); // Even distribution
                total_weight += 100;
            }
            noweight.add(total_weight);
        }
        
        int i = 0;
        // First load up the keys and the index hash (wtostr_index) for randomization to work        
        for (String s : kstr) {
            if (hasvalues) {
                keys.put(s, new Double(Double.parseDouble(vstr[i])));
            } else {
                keys.put(s, new Double(0.0));
            }
            wtostr_index.put(i, s);
            i += 1;
        }
    }
   /**
     * Process each tuple
     *
     * @param payload
     */
    @Override
    public void process(Object payload) {
        // TBD, payload can be either a String or a HashMap
        // Later on add String type to it as the throughput is high
        // The nodes later can split string and construct the HashMap if need be
        // Save I/O
        // For now only HashMap is supported
        //
        // tuple should be "inkey,key" and "value" pair
        for (Map.Entry<String, Double> e : ((HashMap<String, Double>) payload).entrySet()) {
            String inkey = e.getKey();
            ArrayList<Integer> alist = null;
            if (inkeys != null) {
                alist = inkeys.get(e.getKey());
            }
            else {
                alist = noweight;
            }
            // now alist are the weights
            int rval = random.nextInt(alist.get(alist.size()-1));
            int j = 0;
            int wval = 0;
            for (Integer ew : alist) {
                wval += ew.intValue();
                if (wval >= rval) {
                    break;
                }
                j++;
            }
            HashMap<String, Double> tuple = new HashMap<String, Double>();
            String key = wtostr_index.get(j); // the key
            Double keyval = null;
            if (hasvalues) {
                if (voper == value_operation.VOPR_REPLACE) { // replace the incoming value
                    keyval = keys.get(key);                
                }
                else if (voper == value_operation.VOPR_ADD) {
                    keyval = keys.get(key) + e.getValue();
                }
                else if (voper == value_operation.VOPR_MULT) {
                    keyval = keys.get(key) * e.getValue();
                    
                }
                else if (voper == value_operation.VOPR_APPEND) { // not supported yet
                    keyval = keys.get(key);
                }
            }
            else { // pass on the value from incoming tuple
                keyval = e.getValue();
            }
            tuple.put(key + "," + inkey, keyval);            
            emit(OPORT_OUT_DATA, tuple);    
        }   
    }

    /**
     *
     * Checks for user specific configuration values<p>
     *
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

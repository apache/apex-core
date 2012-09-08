/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractInputNode;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Generates synthetic load. Creates tuples and keeps emitting them on the output port "data"<p>
 * <br>
 * The load is generated as per config parameters. This class is mainly meant for testing
 * nodes.<br>
 * It does not need to be windowed. It would just create tuple stream upto the limit set
 * by the config parameters.<br>
 * <br>
 * This node has been benchmarked at over 25 million tuples/second for String objects in local/inline mode<br>
 * <br>
 * <b>Tuple Schema</b>: Has two choices HashMap<String, Double>, or String<br><br>
 * <b>Port Interface</b>:It has only one output port "data" and has no input ports<br><br>
 * <b>Properties</b>:
 * <b>keys</b> is a comma separated list of keys. This key are the <key> field in the tuple<br>
 * <b>values</b> are comma separated list of values. This value is the <value> field in the tuple. If not specified the values for all keys are 0.0<br>
 * <b>weights</b> are comma separated list of probability weights for each key. If not specified the weights are even for all keys<br>
 * <b>tuples_per_sec</b> is the upper limit of number of tuples per sec. The default value is 10000. This library node has been benchmarked at over 25 million tuples/sec<br>
 * <b>string_schema</b> controls the tuple schema. For string set it to "true". By default it is "false" (i.e. HashMap schema)<br>
 * <br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <b>values</b> if specified has to be comma separated doubles and their number must match the number of keys<br>
 * <b>weights</b> if specified has to be comma separated integers and number of their number must match the number of keys<br>
 * <b>tuples_per_sec</b>If specified must be an integer<br>
 * <br>
 *
 * Compile time error checking includes<br>
 * 
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
    int tuples_per_sec = 1;
    HashMap<String, Double> keys = new HashMap<String, Double>();
    HashMap<Integer, String> wtostr_index = new HashMap<Integer, String>();
    ArrayList<Integer> weights = new ArrayList<Integer>();
    
    boolean isstringschema = false;
    int total_weight = 0;
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
    public static final String KEY_TUPLES_PER_SEC = "tuples_per_ms";

    /**
     * If specified as "true" a String class is sent, else HashMap is sent
     */
    public static final String KEY_STRING_SCHEMA = "string_schema";
    
    
    /**
     * Not used, but overridden as it is abstract
     */
    @Override
    public void endWindow() {;
    }

    /**
     * Not used, but overridden as it is abstract
     */
    @Override
    public void beginWindow() {;
    }

    /**
     * 
     * Code to be moved to a proper base method name
     * @param config
     * @return boolean
     */
    public boolean myValidation(NodeConfiguration config) {
        String[] wstr = config.getTrimmedStrings(KEY_WEIGHTS);
        String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        String[] vstr = config.getTrimmedStrings(KEY_VALUES);
        boolean isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);
        
        boolean ret = true;

        if (kstr.length == 0) {
            ret = false;
            throw new IllegalArgumentException("Parameter \"key\" is empty");
        } else {
            LOG.info(String.format("Number of keys are %d", kstr.length));
        }
        
        if (wstr.length == 0) {
            LOG.info("weights was not provided, so keys would be equally weighted");
        } else {
            for (String s : wstr) {
                try {
                    Integer.parseInt(s);
                } catch (NumberFormatException e) {
                    ret = false;
                    throw new IllegalArgumentException(String.format("Weight string should be an integer(%s)", s));
                }   
            }
        }
        if (vstr.length == 0) {
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

        if ((wstr.length != 0) && (wstr.length != kstr.length)) {
            ret = false;
            throw new IllegalArgumentException(
                    String.format("Number of weights (%d) does not match number of keys (%d)",
                    wstr.length, kstr.length));
        }
        if ((vstr.length != 0) && (vstr.length != kstr.length)) {
            ret = false;
            throw new IllegalArgumentException(
                    String.format("Number of values (%d) does not match number of keys (%d)",
                    vstr.length, kstr.length));
        }

        tuples_per_sec = config.getInt(KEY_TUPLES_PER_SEC, 1);
        if (tuples_per_sec <= 0) {
            ret = false;
            throw new IllegalArgumentException(
                    String.format("tuples_per_ms (%d) has to be > 0", tuples_per_sec));
        } else {
            LOG.info(String.format("Using %d tuples per second", tuples_per_sec));
        }
        if (isstringschema) {
            if (vstr.length != 0) {
                LOG.info(String.format("Value %s and stringschema is %s",
                        config.get(KEY_VALUES, ""), config.get(KEY_STRING_SCHEMA, "")));
                ret = false;
                throw new IllegalArgumentException(
                        String.format("Value (\"%s\") cannot be specified if string_schema (\"%s\") is true",
                        config.get(KEY_VALUES, ""), config.get(KEY_STRING_SCHEMA, "")));
            }
        }
        // Should enforce an upper limit
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

        String[] wstr = config.getTrimmedStrings(KEY_WEIGHTS);
        String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        String[] vstr = config.getTrimmedStrings(KEY_VALUES);
        
        isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);
        tuples_per_sec = config.getInt(KEY_TUPLES_PER_SEC, 10000);
        
        // Keys and weights would are accessed via same key
        int i = 0;
        for (String s : kstr) {
            if (wstr.length != 0) {
                weights.add(Integer.parseInt(wstr[i]));
                total_weight += Integer.parseInt(wstr[i]);
            } else {
                total_weight += 100;
            }
            if (vstr.length != 0) {
                keys.put(s, new Double(Double.parseDouble(vstr[i])));
            } else {
                keys.put(s, 0.0);
            }
            wtostr_index.put(i, s);
            i += 1;
        }
    }

    /**
     * 
     * To allow emit to wait till output port is connected in a deployment on Hadoop
     * @param id
     * @param dagpart 
     */
    @Override
    public void connected(String id, Sink dagpart) {
        if (id.equals(OPORT_DATA)) {
            outputConnected = true;
        }
    }

    /**
     * The only way to shut down a loadGenerator. We are looking into a property based shutdown
     */
    @Override
    public void deactivate() {
        shutdown = true;
        super.deactivate();
    }

    /**
     * Generates all the tuples till shutdown (deactivate) is issued
     * @param context 
     */
    @Override
    public void activate(NodeContext context) {
        super.activate(context);
        int i = 0;

        while (!shutdown) {
            if (outputConnected) {
                // send tuples upto tuples_per_sec and then wait for 1 ms
                while (i < tuples_per_sec) {
                    int rval = random.nextInt(total_weight);
                    int j = 0;
                    int wval = 0;
                    for (Integer e : weights) {
                        wval += e.intValue();
                        if (wval >= rval) break;
                        j++;
                    }
                    // wval is the key index
                    if (!isstringschema) {
                        HashMap<String, Double> tuple = new HashMap<String, Double>();
                        String key = wtostr_index.get(j); // the key
                        tuple.put(key, keys.get(key));
                        emit(OPORT_DATA, tuple);
                    }
                    else {
                        emit(OPORT_DATA, wtostr_index.get(j));
                    }
                    i++;
                }
                try {
                    //Thread.sleep(1000);
                    Thread.sleep(10); // Remove sleep if you want to blast data at huge rate
                } catch (InterruptedException e) {
                    LOG.error("Unexpected error while sleeping for 1 s", e);
                }
                i = 0;
            }
        }
        LOG.info("Finished generating tuples");
    }
}

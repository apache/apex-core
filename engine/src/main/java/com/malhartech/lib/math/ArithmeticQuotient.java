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
 * end of window computes the quotient for each key and emits the result on port
 * "quotient".<p> <br> Each stream is added to a hash. The values are added for
 * each key within the window and for each stream.<br> If compute_margin is true
 * then the result is 1 - numerator/denominator expressed as a percentage.
 * Ideally multiply_by should be 1 in this case.<br> This node only functions in
 * a windowed stram application<br> <br> Compile time error processing is done
 * on configuration parameters<br> property <b>compute_margin</b> has to be
 * boolean ("true" or "false").<br> property <b>multiply_by</b> has to be an
 * integer.<br> input ports <b>numerator</b>, <b>denominator</b> must be
 * connected.<br> one of the out bound ports <b>quotient</b> or <b>_error</b>
 * must be connected.<br> <br> Run time error processing are emitted on _error
 * port. The errors are:<br> Divide by zero (Error): no result is emitted on
 * "outport".<br> Input tuple not an integer on denominator stream: This tuple
 * would not be counted towards the result.<br> Input tuple not an integer on
 * numerator stream: This tuple would not be counted towards the result.<br>
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
public class ArithmeticQuotient extends AbstractNode
{
  public static final String IPORT_NUMERATOR = "numerator";
  public static final String IPORT_DENOMINATOR = "denominator";
  public static final String OPORT_QUOTIENT = "quotient";
  private static Logger LOG = LoggerFactory.getLogger(ArithmeticSum.class);
  int mult_by = 1;
  boolean comp_margin = false;
  HashMap<String, Number> numerators = new HashMap<String, Number>();
  HashMap<String, Number> denominators = new HashMap<String, Number>();
  /**
   * Multiplies the quotient by this number. Ease of use for percentage (*
   * 100) or CPM (* 1000)
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
  public void setup(NodeConfiguration config)
  {
    super.setup(config);
    mult_by = config.getInt(KEY_MULTIPLY_BY, 1);
    comp_margin = config.getBoolean(KEY_COMPUTE_MARGIN, false);
  }

  public boolean myValidation(NodeConfiguration config)
  {
    boolean ret = true;

    try {
      mult_by = config.getInt(KEY_MULTIPLY_BY, 1);
    }
    catch (Exception e) {
      ret = false;
      throw new IllegalArgumentException(String.format("key %s (%s) has to be an an integer",
                                                       KEY_MULTIPLY_BY, config.get(KEY_MULTIPLY_BY)));
    }
    return ret;
  }

  @Override
  public void process(Object payload)
  {
    Map<String, Number> active;
    if (IPORT_NUMERATOR.equals(getActivePort())) {
      active = numerators;
    }
    else {
      active = denominators;
    }

    for (Map.Entry<String, Number> e: ((HashMap<String, Number>)payload).entrySet()) {
      Number val = active.get(e.getKey());
      if (val == null) {
        val = e.getValue();
      }
      else {
        val = new Double(val.doubleValue() + e.getValue().doubleValue());
      }
      active.put(e.getKey(), val);
      //LOG.debug("Key was {}, val was {}", e.getKey(), val);
    }
  }

  @Override
  public void endWindow()
  {
    HashMap<String, Number> tuples = new HashMap<String, Number>();
    for (Map.Entry<String, Number> e: denominators.entrySet()) {
      Number nval = numerators.get(e.getKey());
      if (nval == null) {
        tuples.put(e.getKey(), new Double(0.0 * mult_by));
      }
      else {
        tuples.put(e.getKey(), new Double((nval.doubleValue() / e.getValue().doubleValue()) * mult_by));
        numerators.remove(e.getKey()); // so that all left over keys can be reported
      }
    }

    //LOG.debug("emitted {} tuples", denominators.size());

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

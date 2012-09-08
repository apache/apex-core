/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stream.StramTestSupport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Ignore
public class TestArithmeticQuotient
{
  private static Logger LOG = LoggerFactory.getLogger(ArithmeticQuotient.class);

  class TestSink implements Sink
  {
    List<Object> collectedTuples = new ArrayList<Object>();

    @Override
    public void process(Object payload)
    {
      collectedTuples.add(payload);
    }
  }

  /**
   * Test configuration and parameter validation of the node
   */
  @Test
  public void testNodeValidation()
  {
    ArithmeticQuotient node = new ArithmeticQuotient();

    NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());
    conf.set(ArithmeticQuotient.KEY_MULTIPLY_BY, "junk");

    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + ArithmeticQuotient.KEY_MULTIPLY_BY);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + ArithmeticQuotient.KEY_MULTIPLY_BY,
                        e.getMessage().contains("has to be an an integer"));
    }
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing()
  {
    final ArithmeticQuotient node = new ArithmeticQuotient();

    TestSink quotientSink = new TestSink();

    Sink numSink = node.connect(ArithmeticQuotient.IPORT_NUMERATOR, node);
    Sink denSink = node.connect(ArithmeticQuotient.IPORT_DENOMINATOR, node);
    node.connect(ArithmeticQuotient.OPORT_QUOTIENT, quotientSink);

    NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());
    conf.setInt(ArithmeticQuotient.KEY_MULTIPLY_BY, 2);
    node.setup(conf);

    final AtomicBoolean inactive = new AtomicBoolean(true);
    new Thread()
    {
      @Override
      public void run()
      {
        inactive.set(false);
        node.activate(new NodeContext("ArithmeticQuotientTestNode"));
      }
    }.start();

    /**
     * spin while the node gets activated.
     */
    int sleeptimes = 0;
    try {
      do {
        Thread.sleep(20);
        sleeptimes++;
        if (sleeptimes > 20) {
          break;
        }

      }
      while (inactive.get());
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }

    Tuple bt = StramTestSupport.generateBeginWindowTuple("doesn't matter", 1);
    numSink.process(bt);
    denSink.process(bt);

    HashMap<String, Integer> ninput = new HashMap<String, Integer>();
    ninput.put("a", 2);
    ninput.put("b", 20);
    ninput.put("c", 1000);
    numSink.process(ninput);

    HashMap<String, Integer> dinput = new HashMap<String, Integer>();
    dinput.put("a", 2);
    dinput.put("b", 40);
    dinput.put("c", 500);
    denSink.process(dinput);

    Tuple et = StramTestSupport.generateEndWindowTuple("doesn't matter", 1, 1);
    numSink.process(et);
    denSink.process(et);

    // Should get one bag of keys "a", "b", "c"
    try {
      for (int i = 0; i < 10; i++) {
        Thread.sleep(20);
        if (quotientSink.collectedTuples.size() >= 1) {
          break;
        }
      }
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }

    // One for each key
    Assert.assertEquals("number emitted tuples", 3, quotientSink.collectedTuples.size());

    for (Object o: quotientSink.collectedTuples) {
      if (o instanceof Tuple) {
        LOG.debug(o.toString());
      }
      else {
        HashMap<String, Number> output = (HashMap<String, Number>)o;
        for (Map.Entry<String, Number> e: output.entrySet()) {
          LOG.debug(String.format("Key, value is %s,%f", e.getKey(), e.getValue().doubleValue()));
          if (e.getKey().equals("a")) {
            Assert.assertEquals("emitted value for 'a' was ", new Double(2), e.getValue());
          }
          else if (e.getKey().equals("b")) {
            Assert.assertEquals("emitted tuple for 'b' was ", new Double(1), e.getValue());
          }
          else if (e.getKey().equals("c")) {
            Assert.assertEquals("emitted tuple for 'c' was ", new Double(4), e.getValue());
          }
          else {
            LOG.debug(String.format("key was %s", e.getKey()));
          }
        }
      }
    }

  }
}

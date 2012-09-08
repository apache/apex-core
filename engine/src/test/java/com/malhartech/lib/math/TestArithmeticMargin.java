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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestArithmeticMargin
{
  private static Logger LOG = LoggerFactory.getLogger(ArithmeticMargin.class);

  class TestSink implements Sink
  {
    List<Object> collectedTuples = new ArrayList<Object>();

    /**
     *
     * @param payload
     */
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
    ArithmeticMargin node = new ArithmeticMargin();
    NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());

    // no testing as of now as no parameters on this node
    // connectivity should be part of standard tests

  }

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing()
  {
    final ArithmeticMargin node = new ArithmeticMargin();

    TestSink marginSink = new TestSink();

    Sink numSink = node.connect(ArithmeticMargin.IPORT_NUMERATOR, node);
    Sink denSink = node.connect(ArithmeticMargin.IPORT_DENOMINATOR, node);
    node.connect(ArithmeticMargin.OPORT_MARGIN, marginSink);

    NodeConfiguration conf = new NodeConfiguration("mynode", new HashMap<String, String>());
    node.setup(conf);

    final AtomicBoolean inactive = new AtomicBoolean(true);
    new Thread()
    {
      @Override
      public void run()
      {
        inactive.set(false);
        node.activate(new NodeContext("ArithmeticMarginTestNode"));
      }
    }.start();

    /**
     * spin while the node gets activated.
     */
    try {
      do {
        Thread.sleep(20);
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
        if (marginSink.collectedTuples.size() == 1) {
          break;
        }
      }
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }

    // One for each key
    Assert.assertEquals("number emitted tuples", 3, marginSink.collectedTuples.size());

    for (Object o: marginSink.collectedTuples) {
      if (o instanceof Tuple) {
        LOG.debug(o.toString());
      }
      else {
        HashMap<String, Number> output = (HashMap<String, Number>)o;
        for (Map.Entry<String, Number> e: output.entrySet()) {
          LOG.debug(String.format("Key, value is %s,%f", e.getKey(), e.getValue().doubleValue()));
          if (e.getKey().equals("a")) {
            Assert.assertEquals("emitted value for 'a' was ", new Double(0), e.getValue());
          }
          else if (e.getKey().equals("b")) {
            Assert.assertEquals("emitted tuple for 'b' was ", new Double(0.5), e.getValue());
          }
          else if (e.getKey().equals("c")) {
            Assert.assertEquals("emitted tuple for 'c' was ", new Double(-1.0), e.getValue());
          }
          else {
            LOG.debug(String.format("key was %s", e.getKey()));
          }
        }
      }
    }

  }
}

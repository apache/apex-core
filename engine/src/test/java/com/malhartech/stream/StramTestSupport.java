/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stream;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.dag.*;
import com.malhartech.stram.ManualScheduledExecutorService;

/**
 * Bunch of utilities shared between tests.
 */
abstract public class StramTestSupport {

  private static final Logger LOG = LoggerFactory.getLogger( StramTestSupport.class);

  public static Object generateTuple(Object payload, int windowId) {
    return payload;
  }

  public static Tuple generateBeginWindowTuple(String nodeid, int windowId)
  {
    Tuple bwt = new Tuple(Buffer.Data.DataType.BEGIN_WINDOW);
    bwt.setWindowId(windowId);
    return bwt;
  }


  public static Tuple generateEndWindowTuple(String nodeid, int windowId, int tupleCount)
  {
    EndWindowTuple t = new EndWindowTuple();
    t.setTupleCount(tupleCount);
    t.setWindowId(windowId);
    return t;
  }


  public static void checkStringMatch(String print, String expected, String got) {
    assertTrue(
        print + " doesn't match, got: " + got + " expected: " + expected,
        got.matches(expected));
  }

  public static WindowGenerator setupWindowGenerator(ManualScheduledExecutorService mses) {
    WindowGenerator gen = new WindowGenerator(mses);
    StreamConfiguration config = new StreamConfiguration();
    config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0);
    config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);
    gen.setup(config);
    return gen;
  }

  @SuppressWarnings("SleepWhileInLoop")
  public static void waitForWindowComplete(OperatorContext nodeCtx, long windowId) throws InterruptedException
  {
    while (nodeCtx.getLastProcessedWindowId() < windowId) {
      LOG.debug("Waiting for end of window {} at node {}", windowId, nodeCtx.getId());
      Thread.sleep(100);
    }
  }


}

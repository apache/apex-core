/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.support;

import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.StramLocalCluster.LocalStramChild;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.Tuple;
import com.datatorrent.bufferserver.packet.MessageType;
import static java.lang.Thread.sleep;
import junit.framework.AssertionFailedError;
import static org.junit.Assert.assertTrue;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bunch of utilities shared between tests.
 */
abstract public class StramTestSupport
{
  private static final Logger LOG = LoggerFactory.getLogger(StramTestSupport.class);
  public static final long DEFAULT_TIMEOUT_MILLIS = 30000;

  public static Object generateTuple(Object payload, int windowId)
  {
    return payload;
  }

  public static Tuple generateBeginWindowTuple(String nodeid, int windowId)
  {
    Tuple bwt = new Tuple(MessageType.BEGIN_WINDOW);
    bwt.setWindowId(windowId);
    return bwt;
  }

  public static Tuple generateEndWindowTuple(String nodeid, int windowId)
  {
    EndWindowTuple t = new EndWindowTuple(windowId);
    return t;
  }

  public static void checkStringMatch(String print, String expected, String got)
  {
    assertTrue(
            print + " doesn't match, got: " + got + " expected: " + expected,
            got.matches(expected));
  }

  public static WindowGenerator setupWindowGenerator(ManualScheduledExecutorService mses)
  {
    WindowGenerator gen = new WindowGenerator(mses, 1024);
    gen.setResetWindow(0);
    gen.setFirstWindow(0);
    gen.setWindowWidth(1);
    return gen;
  }

  @SuppressWarnings("SleepWhileInLoop")
  public static void waitForWindowComplete(OperatorContext nodeCtx, long windowId) throws InterruptedException
  {
    LOG.debug("Waiting for end of window {} at node {} when lastProcessedWindowId is {}", new Object[]{windowId, nodeCtx.getId(), nodeCtx.getLastProcessedWindowId()});
    long startMillis = System.currentTimeMillis();
    while (nodeCtx.getLastProcessedWindowId() < windowId) {
      if (System.currentTimeMillis() > (startMillis + DEFAULT_TIMEOUT_MILLIS)) {
        long timeout = System.currentTimeMillis() - startMillis;
        throw new AssertionError(String.format("Timeout %s ms waiting for window %s operator %s",  timeout, windowId, nodeCtx.getId()));
      }
      Thread.sleep(20);
    }
  }

  public interface WaitCondition
  {
    boolean isComplete();
  }

  @SuppressWarnings("SleepWhileInLoop")
  public static boolean awaitCompletion(WaitCondition c, long timeoutMillis) throws InterruptedException
  {
    long startMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() < (startMillis + timeoutMillis)) {
      if (c.isComplete()) {
        return true;
      }
      sleep(50);
    }
    return c.isComplete();
  }

  /**
   * Wait until instance of operator is deployed into a container and return the container reference.
   * Asserts non null return value.
   *
   * @param localCluster
   * @param operator
   * @return
   * @throws InterruptedException
   */
  @SuppressWarnings("SleepWhileInLoop")
  public static LocalStramChild waitForActivation(StramLocalCluster localCluster, PTOperator operator) throws InterruptedException
  {
    LocalStramChild container;
    long startMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() < (startMillis + DEFAULT_TIMEOUT_MILLIS)) {
      if (operator.getState() == PTOperator.State.ACTIVE) {
        if ((container = localCluster.getContainer(operator)) != null) {
           return container;
        }
      }
      LOG.debug("Waiting for {}({}) in container {}", new Object[] {operator, operator.getState(), operator.getContainer()});
      Thread.sleep(500);
    }
    throw new AssertionFailedError("timeout waiting for operator deployment " + operator);
  }

  public static class RegexMatcher extends BaseMatcher<String>
  {
    private final String regex;

    public RegexMatcher(String regex) {
        this.regex = regex;
    }

    @Override
    public boolean matches(Object o) {
        return ((String)o).matches(regex);

    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches regex=" + regex);
    }

    public static RegexMatcher matches(String regex) {
        return new RegexMatcher(regex);
    }
  }

}

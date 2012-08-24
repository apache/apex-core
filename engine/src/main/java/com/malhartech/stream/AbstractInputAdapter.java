/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.dag.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
/**
  * Abstract Input Adapter class for implementing various instances of input adapters<p>
  * <br>
  * Provides the hooks needed to integrate with the streaming platform. Manages windows, streams, and context.<br>
  * <br>
  */

public abstract class AbstractInputAdapter implements InputAdapter
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractInputAdapter.class);
  protected StreamContext context;
  protected volatile long baseSeconds;
  protected volatile long windowId;
  protected volatile boolean finished;

  public void emit(Object o)
  {
    Tuple t = new Tuple(o);
    t.setContext(context);
    t.setType(DataType.SIMPLE_DATA);

    synchronized (this) {
      try {
        while (windowId == 0) {
          this.wait();
        }
      }
      catch (InterruptedException ie) {
        logger.info("Interrupted while waiting to be in the window because of {}", ie.getLocalizedMessage());
      }

      t.setWindowId(windowId);
      context.sink(t);
    }
  }

  @Override
  public synchronized void resetWindow(int baseSeconds, int intervalMillis)
  {
    this.baseSeconds = (long) baseSeconds << 32;

    ResetWindowTuple t = new ResetWindowTuple();
    t.setContext(context);
    t.setWindowId(this.baseSeconds | intervalMillis);
    context.sink(t);
  }

  @Override
  public synchronized void beginWindow(int windowId)
  {
    this.windowId = baseSeconds | windowId;

    Tuple t = new Tuple(null);
    t.setType(DataType.BEGIN_WINDOW);
    t.setContext(context);

    t.setWindowId(this.windowId);
    context.sink(t);
    this.notifyAll();
  }

  @Override
  public synchronized void endWindow(int windowId)
  {
    EndWindowTuple t = new EndWindowTuple();
    t.setContext(context);
    t.setWindowId(this.windowId);
    context.sink(t);
    this.windowId = 0;
  }

  public synchronized void endStream()
  {
    EndStreamTuple t = new EndStreamTuple();
    t.setContext(context);

    try {
      while (windowId == 0) {
        this.wait();
      }

      this.wait();
    }
    catch (InterruptedException ie) {
      logger.info("Interrupted while waiting to be in the window because of {}", ie.getLocalizedMessage());
    }

    t.setWindowId(windowId);
    context.sink(t);
    finished = true;
  }

  @Override
  public boolean hasFinished()
  {
    return finished;
  }
}

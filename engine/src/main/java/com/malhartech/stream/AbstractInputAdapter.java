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
public abstract class AbstractInputAdapter implements InputAdapter
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractInputAdapter.class);
  protected StreamContext context;
  protected volatile long timemillis;
  protected volatile boolean finished = false;

  @Override
  public void setContext(StreamContext context)
  {
    this.context = context;
  }

  @Override
  public StreamContext getContext()
  {
    return this.context;
  }

  public void emit(Object o)
  {
    Tuple t = new Tuple(o);
    t.setContext(context);
    t.setType(DataType.SIMPLE_DATA);

    synchronized (this) {
      try {
        while (timemillis == 0) {
          this.wait();
        }
      }
      catch (InterruptedException ie) {
        logger.info("Interrupted while waiting to be in the window because of {}", ie.getLocalizedMessage());
      }

      t.setWindowId(timemillis);
      context.sink(t);
    }
  }

  @Override
  public void beginWindow(long timemillis)
  {
    this.timemillis = timemillis;

    Tuple t = new Tuple(null);
    t.setType(DataType.BEGIN_WINDOW);
    t.setContext(context);

    synchronized (this) {
      t.setWindowId(timemillis);
      context.sink(t);
      this.notifyAll();
    }
  }

  @Override
  public void endWindow(long timemillis)
  {
    this.timemillis = 0;

    EndWindowTuple t = new EndWindowTuple();
    t.setContext(context);

    synchronized (this) {
      t.setWindowId(timemillis);
      context.sink(t);
    }
  }

  public void endStream()
  {
    EndStreamTuple t = new EndStreamTuple();
    t.setContext(context);

    synchronized (this) {
      try {
        while (timemillis == 0) {
          this.wait();
        }
        
        this.wait();
      }
      catch (InterruptedException ie) {
        logger.info("Interrupted while waiting to be in the window because of {}", ie.getLocalizedMessage());
      }

      t.setWindowId(timemillis);
      context.sink(t);
      finished = true;
    }
  }

  @Override
  public boolean hasFinished()
  {
    return finished;
  }
}

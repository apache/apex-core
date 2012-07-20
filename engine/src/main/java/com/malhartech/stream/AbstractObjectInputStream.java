/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.dag.EndWindowTuple;
import com.malhartech.dag.InputAdapter;
import com.malhartech.dag.StreamContext;
import com.malhartech.dag.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractObjectInputStream implements InputAdapter
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractObjectInputStream.class);
  protected StreamContext context;
//  protected volatile long tupleCount = 0;
  protected volatile long timemillis;
  
  public void setContext(StreamContext context)
  {
    this.context = context;
  }
  
  public StreamContext getContext()
  {
    return this.context;
  }
  
  public abstract Object getObject(Object object);
  
  public void sendTuple(Object o)
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
        logger.info("Interrupted while waiting to be in the window because of "
                    + ie.getLocalizedMessage());
      }
      
      t.setWindowId(timemillis);
//      tupleCount++;
      context.sink(t);
    }
  }
  
  public void beginWindow(long timemillis)
  {
    this.timemillis = timemillis;
    
    Tuple t = new Tuple(null);
    t.setType(DataType.BEGIN_WINDOW);
    t.setContext(context);
    
    synchronized (this) {
      t.setWindowId(timemillis);
//      tupleCount = 0;
      context.sink(t);
      this.notifyAll();
    }
  }
  
  public void endWindow(long timemillis)
  {
    this.timemillis = 0;
    
    EndWindowTuple t = new EndWindowTuple();
    t.setContext(context);
    
    synchronized (this) {
//      t.setTupleCount(tupleCount);
      t.setWindowId(timemillis);
      context.sink(t);
    }
    
  }
  
  public boolean hasFinished()
  {
    return false;
  }
}

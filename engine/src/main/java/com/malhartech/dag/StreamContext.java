/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the destination for tuples processed.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamContext implements Context
{
  private final Logger LOG = LoggerFactory.getLogger(StreamContext.class);
  private Sink sink;
  private SerDe serde;
  private long windowId;
  private int tupleCount;

  /**
   * @param sink - target node, not required for output adapter
   */
  public void setSink(Sink sink)
  {
    LOG.info("sink: {}", sink);
    this.sink = sink;
  }

  public SerDe getSerDe()
  {
    return serde; // required for socket connection
  }

  public void setSerde(SerDe serde)
  {
    this.serde = serde;
  }

  public void sink(Tuple t)
  {
    //LOG.info(this + " " + t);
    switch (t.getType()) {
      case SIMPLE_DATA:
      case PARTITIONED_DATA:
        tupleCount++;
        break;

      case BEGIN_WINDOW:
        tupleCount = 0;
        break;

      case END_WINDOW:
        if (tupleCount != ((EndWindowTuple) t).getTupleCount()) {
          EndWindowTuple ewt = new EndWindowTuple();
          ewt.setTupleCount(tupleCount);
          ewt.setWindowId(t.getWindowId());
          t = ewt;
          break;
        }
    }

    t.setContext(this);
    sink.doSomething(t);
  }

  public long getWindowId()
  {
    return windowId;
  }

  public void setWindowId(long windowId)
  {
    this.windowId = windowId;
  }

  public int getTupleCount()
  {
    return tupleCount;
  }
}

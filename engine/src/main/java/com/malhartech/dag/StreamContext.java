/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.EndWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the destination for tuples processed.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamContext implements Context
{
  private final Logger logger = LoggerFactory.getLogger(StreamContext.class);
  private Sink sink;
  private SerDe serde;
  private long windowId;
  private int tupleCount;

  /**
   * @param sink - target node, not required for output adapter
   */
  public void setSink(Sink sink)
  {
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
    switch (t.getData().getType()) {
      case SIMPLE_DATA:
      case PARTITIONED_DATA:
        tupleCount++;
        break;

      case BEGIN_WINDOW:
        tupleCount = 0;
        break;

      case END_WINDOW:
        if (tupleCount == t.getData().getEndwindow().getTupleCount()) {
          logger.debug("No need to create a new tuple");
        }
        else {
          logger.debug("creating a new tuple since working on partitioned data " + t.getData());
          Data.Builder db = Data.newBuilder();
          db.setWindowId(t.getData().getWindowId());
          db.setType(Data.DataType.END_WINDOW);

          EndWindow.Builder eb = EndWindow.newBuilder();
          eb.setNode("?@?");
          eb.setTupleCount(tupleCount);

          t.setData(db.build());
        }
        break;
    }

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

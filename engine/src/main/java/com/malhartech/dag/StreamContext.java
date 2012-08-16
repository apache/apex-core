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
  /**
   * @return the startingWindowId
   */
  public long getStartingWindowId()
  {
    return startingWindowId;
  }

  /**
   * @param startingWindowId the startingWindowId to set
   */
  public void setStartingWindowId(long startingWindowId)
  {
    this.startingWindowId = startingWindowId;
  }

  /**
   * @return the sourceId
   */
  public String getSourceId()
  {
    return sourceId;
  }

  /**
   * @param sourceId the sourceId to set
   */
  public void setSourceId(String upstreamNodeId)
  {
    this.sourceId = upstreamNodeId;
  }

  /**
   * @return the sinkId
   */
  public String getSinkId()
  {
    return sinkId;
  }

  /**
   * @param sinkId the sinkId to set
   */
  public void setSinkId(String downstreamNodeId)
  {
    this.sinkId = downstreamNodeId;
  }

  public static enum State
  {
    UNDEFINED,
    OUTSIDE_WINDOW,
    INSIDE_WINDOW,
    TERMINATED
  }
  private final Logger LOG = LoggerFactory.getLogger(StreamContext.class);
  private Sink sink;
  private SerDe serde;
  private int tupleCount;
  private State sinkState;
  private String sourceId;
  private String sinkId;
  private long startingWindowId;

  public StreamContext(String sourceId, String sinkId)
  {
    this.sourceId = sourceId;
    this.sinkId = sinkId;
    sinkState = State.UNDEFINED;
  }

  /**
   * @param sink - target node, not required for output adapter
   */
  public void setSink(final Sink sink)
  {
    LOG.debug("setSink: {}", sink);
    this.sink = startingWindowId > 0
                ? new Sink()
    {
      @Override
      public void doSomething(Tuple t)
      {
        if (startingWindowId <= t.getWindowId()) {
          LOG.debug("Sink {} kicking in after window {}", sink, startingWindowId);
          StreamContext.this.sink = sink;
          sink.doSomething(t);
        }
      }
    }
                : sink;
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

  public int getTupleCount()
  {
    return tupleCount;
  }

  State getSinkState()
  {
    return sinkState;
  }

  void setSinkState(State state)
  {
    sinkState = state;
  }

  @Override
  public String toString()
  {
    return " tuples = " + tupleCount + " state = " + sinkState;
  }
}

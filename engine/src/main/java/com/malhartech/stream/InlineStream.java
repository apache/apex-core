/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.engine.DefaultReservoir;
import com.malhartech.engine.Reservoir;
import com.malhartech.engine.Stream;
import com.malhartech.engine.StreamContext;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
/**
 *
 * Inline streams are used for performance enhancement when both the operators are in the same hadoop container<p>
 * <br>
 * Inline is a hint that the stram can choose to ignore. Stram may also convert a normal stream into an inline one
 * for performance reasons. A stream tagged with persist flag will not be inlined, as persistence requires a buffer
 * server<br>
 * Inline streams currently cannot be partitioned. Since the main reason for partitioning
 * is to load balance and that means across different hadoop containers. In future we may take a look at it.<br>
 * <br>
 *
 */
public class InlineStream implements Stream<Object>
{
  private DefaultReservoir reservoir;

  /**
   *
   * @param context
   */
  @Override
  public void setup(StreamContext context)
  {
    // nothing to be done here.
  }

  /**
   *
   * @param context
   */
  @Override
  public void activate(StreamContext context)
  {
  }

  /**
   *
   */
  @Override
  public void deactivate()
  {
  }

  /**
   *
   */
  @Override
  public void teardown()
  {
  }

  /**
   *
   * @param payload
   */
  @Override
  public final void process(Object payload)
  {
    try {
      reservoir.put(payload);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return false;
  }

  @Override
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public Reservoir getReservoir(String sinkId, int capacity)
  {
    if (reservoir == null) {
      reservoir = new DefaultReservoir(sinkId, capacity);
    }

    return reservoir;
  }

}

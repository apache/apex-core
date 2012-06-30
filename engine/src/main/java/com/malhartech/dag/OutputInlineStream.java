/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.stram.StreamContext;
import java.util.Collection;

/**
 *
 * @author chetan
 */
public class OutputInlineStream implements Sink
{

  private final StreamContext context;
  private final Collection<Tuple> collection;

  public OutputInlineStream(StreamContext context, Collection<Tuple> collection)
  {
    this.context = context;
    this.collection = collection;
  }

  public void doSomething(Tuple t)
  {
    synchronized (collection) {
      collection.add(t);
      collection.notify();
    }
  }

  public StreamContext getStreamContext()
  {
    return context;
  }
}

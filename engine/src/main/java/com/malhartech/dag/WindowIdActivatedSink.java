/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WindowIdActivatedSink implements Sink
{
  private final Sink ultimateSink;
  private final long windowId;

  public WindowIdActivatedSink(final Sink sink, final long windowId)
  {
    this.ultimateSink = sink;
    this.windowId = windowId;
  }

  @Override
  public void process(Object payload) throws MutatedSinkException
  {
    if (payload instanceof Tuple
            && ((Tuple)payload).getType() == Buffer.Data.DataType.BEGIN_WINDOW
            && ((Tuple)payload).getWindowId() >= windowId) {
      throw new MutatedSinkException(this, ultimateSink, "Sink activated at beginning of window " + windowId);
    }
  }
}

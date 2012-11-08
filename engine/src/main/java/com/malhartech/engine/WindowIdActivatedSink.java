/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.bufferserver.Buffer;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WindowIdActivatedSink implements Sink
{
  private final Sink sink;
  private final long windowId;
  private final Stream stream;
  private final String identifier;

  public WindowIdActivatedSink(Stream stream, String identifier, final Sink sink, final long windowId)
  {
    this.stream = stream;
    this.identifier = identifier;
    this.sink = sink;
    this.windowId = windowId;
  }

  @Override
  public void process(Object payload)
  {
    if (payload instanceof Tuple
            && ((Tuple)payload).getType() == Buffer.Data.DataType.BEGIN_WINDOW
            && ((Tuple)payload).getWindowId() > windowId) {
      sink.process(payload);
      stream.setSink(identifier, sink);
    }
  }
}

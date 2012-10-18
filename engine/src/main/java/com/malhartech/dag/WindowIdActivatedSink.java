/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

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
  private final String port;
  private final Component source;

  public WindowIdActivatedSink(Component source, String port, final Sink sink, final long windowId)
  {
    this.source = source;
    this.port = port;
    this.sink = sink;
    this.windowId = windowId;
  }

  @Override
  public void process(Object payload)
  {
    if (payload instanceof Tuple
            && ((Tuple)payload).getType() == Buffer.Data.DataType.BEGIN_WINDOW
            && ((Tuple)payload).getWindowId() >= windowId) {
      sink.process(payload);
      source.connect(port, sink);
    }
  }
}

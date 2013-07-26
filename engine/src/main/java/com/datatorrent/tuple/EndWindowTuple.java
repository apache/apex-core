/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.tuple;

import com.datatorrent.bufferserver.packet.MessageType;

/**
 *
 * End of window tuple<p>
 * <br>
 * This defines the end of a window. A new begin window has to come after the end window of the previous window<br>
 * <br>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class EndWindowTuple extends Tuple
{
  public EndWindowTuple(long windowId)
  {
    super(MessageType.END_WINDOW);
    this.windowId = windowId;
  }
}

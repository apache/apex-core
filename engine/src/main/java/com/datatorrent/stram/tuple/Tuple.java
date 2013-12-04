/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.tuple;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.util.Codec;

/**
 *
 * Basic object that is streamed<p>
 * <br>
 * Tuples are of the following type<br>
 * Data:<br>
 * Control: begin window, end window, reset window, end stream<br>
 * heartbeat: To be done, not a high priority<br>
 * <br>
 *
 * @author chetan
 * @since 0.3.2
 */
public class Tuple
{
  protected long windowId;
  private final MessageType type;

  public Tuple(MessageType t, long windowId)
  {
    if (windowId < 0) {
      throw new AssertionError("Window ID is negative (" + windowId + ")");
    }
    type = t;
    this.windowId = windowId;
  }

  /**
   * @return the windowId
   */
  public long getWindowId()
  {
    return windowId;
  }

  /**
   * @return the type
   */
  public MessageType getType()
  {
    return type;
  }

  @Override
  public String toString()
  {
    return "type = " + type + " " + Codec.getStringWindowId(windowId);
  }

}

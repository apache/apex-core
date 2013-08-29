/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.stram.tuple;

import com.datatorrent.bufferserver.packet.MessageType;

/**
 * Defines end of streaming tuple<p>
 * <br>
 * This is needed to shutdown a stream dynamically. Shutting down a dag can also
 * be done dynamically by shutting down all the input streams (all inputadapters).<br>
 * <br>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class EndStreamTuple extends Tuple
{
  public EndStreamTuple(long windowId)
  {
    super(MessageType.END_STREAM);
    this.windowId = windowId;
  }

}

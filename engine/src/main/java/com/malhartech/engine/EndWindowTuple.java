/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.bufferserver.packet.MessageType;

/**
 *
 * End of window tuple<p>
 * <br>
 * This defines the end of a window. A new begin window has to come after the end window of the previous window<br>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class EndWindowTuple extends Tuple
{
  public EndWindowTuple()
  {
    super(MessageType.END_WINDOW);
  }
}

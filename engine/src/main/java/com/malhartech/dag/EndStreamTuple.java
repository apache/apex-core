/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer;

/**
 * Defines end of streaming tuple. This is needed to shutdown a stream dynamically. Shutting down a dag can also
 * be done dynamically by shutting down all the input streams (all inputadapters).
 *
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class EndStreamTuple extends Tuple
{
  public EndStreamTuple()
  {
    super(Buffer.Data.DataType.END_STREAM);
  }

}

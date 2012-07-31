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
public class EndStreamTuple extends Tuple
{
  public EndStreamTuple()
  {
    super(null);
    super.setType(Buffer.Data.DataType.END_STREAM);
  }

}

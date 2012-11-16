/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.bufferserver.Buffer.Data.DataType;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class CodecStateTuple extends Tuple
{
  public final byte[] state;

  public CodecStateTuple(long windowId, byte[] state)
  {
    super(DataType.CODEC_STATE);
    this.windowId = windowId;
    this.state = state;
  }
}

/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface ByteCounterStream extends Stream
{
  public long getByteCount(boolean reset);
}

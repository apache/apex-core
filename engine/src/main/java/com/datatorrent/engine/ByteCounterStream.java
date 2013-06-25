/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.engine;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public interface ByteCounterStream extends Stream
{
  public long getByteCount(boolean reset);
}

/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.engine;

/**
 * <p>ByteCounterStream interface.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public interface ByteCounterStream extends Stream
{
  public long getByteCount(boolean reset);
}

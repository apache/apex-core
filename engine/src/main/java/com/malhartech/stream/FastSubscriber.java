/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class FastSubscriber extends BufferServerSubscriber
{
  public FastSubscriber(String id, int queueCapacity)
  {
    super(id, queueCapacity);
  }

  @Override
  public int readSize()
  {
    if (writeOffset - readOffset < 2) {
      return -1;
    }

    short s = buffer[readOffset++];
    return s | (buffer[readOffset++] << 8);
  }
}

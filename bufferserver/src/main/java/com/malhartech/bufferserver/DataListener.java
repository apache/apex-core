/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 *
 * @author chetan
 */
public interface DataListener
{

  public static final ByteBuffer NULL_PARTITION = ByteBuffer.allocate(0);

  public void dataAdded(ByteBuffer partition);

  public int getPartitions(Collection<ByteBuffer> partitions);
}

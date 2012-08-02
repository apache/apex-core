/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.util.SerializedData;
import java.nio.ByteBuffer;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface DataIntrospector
{
  public DataType getType(SerializedData data);

  public long getWindowId(SerializedData data);

  public ByteBuffer getPartitionedData(SerializedData data);

  public void wipeData(SerializedData previous);
}

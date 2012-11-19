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
 * Looks at the data and provide information about it<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface DataIntrospector
{
    /**
     *
     * @param data
     * @return DataType
     */
  public DataType getType(SerializedData data);

  /**
   *
   * @param data
   * @return long
   */
  public int getWindowId(SerializedData data);

  /**
   *
   * @param data
   * @return Object
   */
  public Object getData(SerializedData data);

  /**
   *
   * @param previous
   */
  public void wipeData(SerializedData previous);
}

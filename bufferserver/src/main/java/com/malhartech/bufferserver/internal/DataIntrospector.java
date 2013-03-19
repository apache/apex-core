/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.bufferserver.util.SerializedData;

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
     * @return MessageType
     */
  public MessageType getType(SerializedData data);

  /**
   *
   * @param data
   * @return long
   */
  public int getWindowId(SerializedData data);

  public int getBaseSeconds(SerializedData data);

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

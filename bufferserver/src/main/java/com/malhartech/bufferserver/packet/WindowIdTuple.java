/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WindowIdTuple extends Tuple
{
  public WindowIdTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  @Override
  public int getWindowId()
  {
    return readVarInt(offset + 1, offset + length);
  }

  @Override
  public MessageType getType()
  {
    return MessageType.valueOf(buffer[offset]);
  }

  @Override
  public int getPartition()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getDataOffset()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getBaseSeconds()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getWindowWidth()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public String toString()
  {
    return "WindowIdTuple{" + getType() + ", " + Integer.toHexString(getWindowId()) + '}';
  }

}

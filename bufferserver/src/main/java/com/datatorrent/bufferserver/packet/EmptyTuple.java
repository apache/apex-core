/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

import com.datatorrent.common.util.Slice;

/**
 * <p>EmptyTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class EmptyTuple extends Tuple
{
  public EmptyTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  @Override
  public MessageType getType()
  {
    return MessageType.NO_MESSAGE;
  }

  @Override
  public int getWindowId()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getPartition()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Slice getData()
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

  public static byte[] getSerializedTuple(byte value)
  {
    return new byte[] {value};
  }

}

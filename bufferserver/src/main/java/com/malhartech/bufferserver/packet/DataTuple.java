/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

import com.malhartech.util.Fragment;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class DataTuple extends Tuple
{
  public DataTuple(byte[] array, int offset, int index)
  {
    super(array, offset, index);
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
  public Fragment getData()
  {
    return new Fragment(buffer, offset + 1, length - 1);
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

  public static byte[] getSerializedTuple(byte type, Fragment f)
  {
    byte[] array = new byte[f.length + 1];
    array[0] = type;
    System.arraycopy(f.buffer, f.offset, array, 1, f.length);
    return array;
  }

}

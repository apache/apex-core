/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

import com.datatorrent.common.util.Slice;

/**
 * <p>Abstract RequestTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public abstract class RequestTuple extends Tuple
{
  protected boolean valid;
  protected boolean parsed;

  public RequestTuple(byte[] buffer, int offset, int length)
  {
    super(buffer, offset, length);
  }

  public boolean isValid()
  {
    return valid;
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
  public int getWindowWidth()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public abstract void parse();

  public abstract String getVersion();

  public abstract String getIdentifier();

}

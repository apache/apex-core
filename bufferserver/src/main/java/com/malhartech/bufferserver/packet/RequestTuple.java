/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

import com.malhartech.netlet.Client.Fragment;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
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
  public Fragment getData()
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

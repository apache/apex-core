/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

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

  public abstract void parse();

  public abstract String getVersion();

  public abstract String getIdentifier();

}

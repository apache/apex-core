/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.support;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Controller extends com.malhartech.bufferserver.client.Controller
{
  public Fragment data;

  public Controller(String id)
  {
    super(id);
  }

  @Override
  public void purge(String sourceId, long windowId)
  {
    data = null;
    super.purge(sourceId, windowId);
  }

  @Override
  public void reset(String sourceId, long windowId)
  {
    data = null;
    super.reset(sourceId, windowId);
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    data = new Fragment(buffer, offset, size);
  }

}

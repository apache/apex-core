/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.support;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class Controller extends com.datatorrent.bufferserver.client.Controller
{
  public String data;

  public Controller(String id)
  {
    super(id);
  }

  @Override
  public void purge(String version, String sourceId, long windowId)
  {
    data = null;
    super.purge(version, sourceId, windowId);
  }

  @Override
  public void reset(String version, String sourceId, long windowId)
  {
    data = null;
    super.reset(version, sourceId, windowId);
  }

  @Override
  public void onMessage(String message)
  {
    data = message;
  }

}

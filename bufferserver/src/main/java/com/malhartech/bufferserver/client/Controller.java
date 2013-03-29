/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.malhartech.bufferserver.packet.PurgeRequestTuple;
import com.malhartech.bufferserver.packet.ResetRequestTuple;
import java.io.IOException;
import malhar.netlet.DefaultEventLoop;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class Controller extends AbstractClient
{
  String id;

  public Controller(String id)
  {
    this.id = id;
  }

  public void purge(String sourceId, long windowId)
  {
    write(PurgeRequestTuple.getSerializedRequest(sourceId, windowId));
  }

  public void reset(String sourceId, long windowId)
  {
    write(ResetRequestTuple.getSerializedRequest(sourceId, windowId));
  }

  @Override
  public String toString()
  {
    return "Controller{" + "id=" + id + '}';
  }

}

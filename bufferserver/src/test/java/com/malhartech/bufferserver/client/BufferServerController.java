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
public class BufferServerController extends AbstractSocketSubscriber
{
  private final String sourceId;
  public long windowId;
  public Fragment data;

  public BufferServerController(String sourceId)
  {
    this.sourceId = sourceId;
  }

  public void purge()
  {
    data = null;
    write(PurgeRequestTuple.getSerializedRequest(sourceId, windowId));
  }

  public void reset()
  {
    data = null;
    write(ResetRequestTuple.getSerializedRequest(sourceId, windowId));
  }

  @Override
  public void handleException(Exception cce, DefaultEventLoop el)
  {
    if (cce instanceof IOException) {
      el.disconnect(this);
    }
    else {
      throw new RuntimeException(cce);
    }
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    data = new Fragment(buffer, offset, size);
  }

}

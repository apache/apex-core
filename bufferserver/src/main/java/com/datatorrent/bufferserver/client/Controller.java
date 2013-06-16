/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.bufferserver.packet.PurgeRequestTuple;
import com.malhartech.bufferserver.packet.ResetRequestTuple;
import com.malhartech.bufferserver.packet.Tuple;
import com.malhartech.common.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class Controller extends AbstractClient
{
  String id;

  public Controller(String id)
  {
    super(1024, 1024);
    this.id = id;
  }

  public void purge(String version, String sourceId, long windowId)
  {
    write(PurgeRequestTuple.getSerializedRequest(version, sourceId, windowId));
    logger.debug("Sent purge request sourceId = {}, windowId = {}", sourceId, windowId);
  }

  public void reset(String version, String sourceId, long windowId)
  {
    write(ResetRequestTuple.getSerializedRequest(version, sourceId, windowId));
    logger.debug("Sent reset request sourceId = {}, windowId = {}", sourceId, windowId);
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    Tuple t = Tuple.getTuple(buffer, offset, size);
    assert (t.getType() == MessageType.PAYLOAD);
    Slice f = t.getData();
    onMessage(new String(f.buffer, f.offset, f.length));
  }

  public abstract void onMessage(String message);

  @Override
  public String toString()
  {
    return "Controller{" + "id=" + id + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(Controller.class);
}

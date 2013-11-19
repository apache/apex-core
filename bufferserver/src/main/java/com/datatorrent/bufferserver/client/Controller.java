/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.client;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.PurgeRequestTuple;
import com.datatorrent.bufferserver.packet.ResetRequestTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.common.util.Slice;
import com.datatorrent.netlet.AbstractLengthPrependerClient;

/**
 * <p>Abstract Controller class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public abstract class Controller extends AbstractLengthPrependerClient
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

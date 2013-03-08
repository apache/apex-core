/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.client.ClientHandler;
import com.google.protobuf.ByteString;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class BufferServerPublisher extends AbstractSocketPublisher
{
  private static final Logger logger = LoggerFactory.getLogger(BufferServerPublisher.class);
  private final String id;
  public int baseWindow;
  public int windowId;

  public BufferServerPublisher(String id)
  {
    this.id = id;
  }

  public void publishMessage(Object payload)
  {
    Buffer.Message.Builder db = Buffer.Message.newBuilder();
    if (payload instanceof Tuple) {
      final Tuple t = (Tuple)payload;
      db.setType(t.getType());

      switch (t.getType()) {
        case BEGIN_WINDOW:
          Buffer.BeginWindow.Builder bw = Buffer.BeginWindow.newBuilder();
          bw.setWindowId(windowId = (int)t.getWindowId());
          db.setBeginWindow(bw);
          break;

        case END_WINDOW:
          Buffer.EndWindow.Builder ew = Buffer.EndWindow.newBuilder();
          ew.setWindowId(windowId = (int)t.getWindowId());
          db.setEndWindow(ew);
          break;

        case END_STREAM:
          Buffer.EndStream.Builder es = Buffer.EndStream.newBuilder();
          es.setWindowId(windowId = (int)t.getWindowId());
          db.setEndStream(es);
          break;

        case RESET_WINDOW:
          Buffer.ResetWindow.Builder rw = Buffer.ResetWindow.newBuilder();
          rw.setWidth(t.getIntervalMillis());
          rw.setBaseSeconds(t.getBaseSeconds());
          db.setResetWindow(rw);
          break;

        default:
          throw new UnsupportedOperationException("this data type is not handled in the stream");
      }
    }
    else {
      Buffer.Payload.Builder sdb = Buffer.Payload.newBuilder();
      sdb.setData(ByteString.copyFrom((byte[])payload));
      sdb.setPartition(0);

      db.setType(Buffer.Message.MessageType.PAYLOAD);
      db.setPayload(sdb);
    }

//    logger.debug("write with data {}", db.build());
    try {
      channel.write(db.build());
    }
    catch (RejectedExecutionException ree) {

    }
  }

  /**
   *
   */
  @Override
  public void activate()
  {
    super.activate();
//    logger.debug("registering publisher: {}", id);
    ClientHandler.publish(channel, id, "BufferServerPublisher", (long)baseWindow << 32 | windowId);
  }
}

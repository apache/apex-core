/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

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
  int baseWindow;
  int windowId;

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
      db.setWindowId((int)t.getWindowId());

      switch (t.getType()) {
        case BEGIN_WINDOW:
          this.windowId = db.getWindowId();
          break;

        case END_WINDOW:
          break;

        case END_STREAM:
          break;

        case RESET_WINDOW:
          Buffer.ResetWindow.Builder rw = Buffer.ResetWindow.newBuilder();
          rw.setWidth(t.getIntervalMillis());
          db.setWindowId(t.getBaseSeconds());
          db.setResetWindow(rw);
          break;

        default:
          throw new UnsupportedOperationException("this data type is not handled in the stream");
      }
    }
    else {
      db.setWindowId(this.windowId);
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

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.ByteString;
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
  private int windowId;

  public BufferServerPublisher(String id)
  {
    this.id = id;
  }

  public void publishMessage(Object payload)
  {
    Buffer.Data.Builder db = Buffer.Data.newBuilder();
    if (payload instanceof Tuple) {
      final Tuple t = (Tuple)payload;
      db.setType(t.getType());
      db.setWindowId((int)t.getWindowId());

      switch (t.getType()) {
        case BEGIN_WINDOW:
          Buffer.BeginWindow.Builder bw = Buffer.BeginWindow.newBuilder();
          bw.setNode("SOS");
          db.setBeginWindow(bw);
          this.windowId = db.getWindowId();
          break;

        case END_WINDOW:
          Buffer.EndWindow.Builder ew = Buffer.EndWindow.newBuilder();
          ew.setNode("SOS");
          ew.setTupleCount(0);

          db.setEndWindow(ew);
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
      Buffer.SimpleData.Builder sdb = Buffer.SimpleData.newBuilder();
      sdb.setData(ByteString.copyFrom((byte[])payload));

      db.setType(Buffer.Data.DataType.SIMPLE_DATA);
      db.setSimpleData(sdb);
    }

    logger.debug("write with data {}", db.build());
    channel.write(db.build());
  }

  /**
   *
   */
  @Override
  public void activate()
  {
    super.activate();
    logger.debug("registering publisher: {}", id);
    ClientHandler.publish(channel, id, "BufferServerPublisher", 0);
  }
}

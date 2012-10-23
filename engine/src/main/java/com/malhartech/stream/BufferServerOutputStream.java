/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.google.protobuf.ByteString;
import com.malhartech.api.Sink;
import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.dag.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements tuple flow of node to then buffer server in a logical stream<p>
 * <br>
 * Extends SocketOutputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a write instance of a stream and hence would take care of persistence and retaining tuples till they are consumed<br>
 * Partitioning is managed by this instance of the buffer server<br>
 * <br>
 */
public class BufferServerOutputStream extends SocketOutputStream
{
  private static final Logger logger = LoggerFactory.getLogger(BufferServerOutputStream.class);
  SerDe serde;
  int windowId;
  private long count;

  // see if you can do this in setup instead of in constructor.
  public BufferServerOutputStream(SerDe serde) {
    this.serde = serde;
  }
  /**
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    count++;
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
          ew.setTupleCount(((EndWindowTuple)t).getTupleCount());

          db.setEndWindow(ew);
          break;

        case END_STREAM:
          break;

        case RESET_WINDOW:
          Buffer.ResetWindow.Builder rw = Buffer.ResetWindow.newBuilder();
          rw.setWidth(((ResetWindowTuple)t).getIntervalMillis());
          db.setWindowId(((ResetWindowTuple)t).getBaseSeconds());
          db.setResetWindow(rw);
          break;

        default:
          throw new UnsupportedOperationException("this data type is not handled in the stream");
      }
    }
    else {
      db.setWindowId(this.windowId);
      byte partition[] = serde.getPartition(payload);
      if (partition == null) {
        Buffer.SimpleData.Builder sdb = Buffer.SimpleData.newBuilder();
        sdb.setData(ByteString.copyFrom(serde.toByteArray(payload)));

        db.setType(Buffer.Data.DataType.SIMPLE_DATA);
        db.setSimpleData(sdb);
      }
      else {
        Buffer.PartitionedData.Builder pdb = Buffer.PartitionedData.newBuilder();
        pdb.setPartition(ByteString.copyFrom(partition));
        pdb.setData(ByteString.copyFrom(serde.toByteArray(payload)));

        db.setType(Buffer.Data.DataType.PARTITIONED_DATA);
        db.setPartitionedData(pdb);
      }
    }

//    logger.debug("{} channel write with data {}", getContext(), db.build());
    channel.write(db.build());
  }

  /**
   *
   */
  @Override
  public void activated(StreamContext context)
  {
    super.activated(context);
    logger.debug("registering publisher: {} {} windowId={}", new Object[]{context.getSourceId(), context.getId(), context.getStartingWindowId()});
    ClientHandler.publish(channel, context.getSourceId(), context.getId(), context.getStartingWindowId());
  }

  @Override
  public Sink setSink(String id, Sink sink)
  {
    throw new IllegalAccessError("Attempt to set destination other than buffer server on " + this + " stream!");
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return false;
  }

  @Override
  public long getProcessedCount()
  {
    return count;
  }
}

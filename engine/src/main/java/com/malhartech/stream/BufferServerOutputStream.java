/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.google.protobuf.ByteString;
import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.Buffer.Message.Builder;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.engine.ResetWindowTuple;
import com.malhartech.engine.StreamContext;
import com.malhartech.engine.Tuple;
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
public class BufferServerOutputStream extends SocketOutputStream<Object>
{
  private static final Logger logger = LoggerFactory.getLogger(BufferServerOutputStream.class);
  public static final int BUFFER_SIZE = 64 * 1024;
  StreamCodec<Object> serde;
  int windowId;
  int writtenBytes;

  protected void write(Builder db) throws RuntimeException
  {
    Message d = db.build();
    if (writtenBytes > BUFFER_SIZE) {
      try {
        channel.flush().await();
        writtenBytes = 0;
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }

    channel.write(d);
    writtenBytes += d.getSerializedSize();
  }

  public BufferServerOutputStream(StreamCodec<Object> serde)
  {
    this.serde = serde;
  }

  /**
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    Buffer.Message.Builder db = Buffer.Message.newBuilder();
    if (payload instanceof Tuple) {
      final Tuple t = (Tuple)payload;
      db.setType(t.getType());

      switch (t.getType()) {
        case CHECKPOINT:
          serde.resetState();
          break;

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
          rw.setWidth(((ResetWindowTuple)t).getIntervalMillis());
          rw.setBaseSeconds(((ResetWindowTuple)t).getBaseSeconds());
          db.setResetWindow(rw);
          break;

        default:
          throw new UnsupportedOperationException("this data type is not handled in the stream");
      }
    }
    else {
      DataStatePair dsp = serde.toByteArray(payload);

      /*
       * if there is any state write that for the subscriber before we write the data.
       */
      if (dsp.state != null) {
        write(Buffer.Message.newBuilder().setType(MessageType.CODEC_STATE)
                .setCodecState(Buffer.CodecState.newBuilder().setData(ByteString.copyFrom(dsp.state))));
      }

      /*
       * Now that the state if any has been sent, we can proceed with the actual data we want to send.
       */
      Buffer.Payload.Builder pdb = Buffer.Payload.newBuilder();
      pdb.setPartition(serde.getPartition(payload));
      pdb.setData(ByteString.copyFrom(dsp.data));
      db.setType(Buffer.Message.MessageType.PAYLOAD);
      db.setPayload(pdb);
    }

    write(db);
  }

  /**
   *
   * @param context
   */
  @Override
  public void activate(StreamContext context)
  {
    logger.debug("registering publisher: {} {} windowId={} server={}", new Object[] {context.getSourceId(), context.getId(), context.getStartingWindowId(), context.getBufferServerAddress()});
    super.activate(context);
    ClientHandler.publish(channel, context.getSourceId(), context.getId(), context.getStartingWindowId());
  }

  @Override
  public void setSink(String id, Sink<Object> sink)
  {
    throw new IllegalAccessError("Attempt to set destination other than buffer server on " + this + " stream!");
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return false;
  }

}

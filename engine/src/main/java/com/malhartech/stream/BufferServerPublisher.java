/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.bufferserver.client.Publisher;
import com.malhartech.bufferserver.packet.*;
import com.malhartech.engine.Stream;
import com.malhartech.engine.StreamContext;
import com.malhartech.engine.Tuple;
import com.malhartech.netlet.EventLoop;
import static java.lang.Thread.sleep;
import java.net.InetSocketAddress;
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
public class BufferServerPublisher extends Publisher implements Stream<Object>
{
  StreamCodec<Object> serde;
  int writtenBytes;
  int windowId;
  private EventLoop eventloop;

  public BufferServerPublisher(String sourceId)
  {
    super(sourceId);
  }

  /**
   *
   * @param payload
   */
  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void process(Object payload)
  {
    byte[] array;
    if (payload instanceof Tuple) {
      final Tuple t = (Tuple)payload;

      switch (t.getType()) {
        case CHECKPOINT:
          serde.resetState();
          array = EmptyTuple.getSerializedTuple(MessageType.CHECKPOINT_VALUE);
          break;

        case BEGIN_WINDOW:
          array = BeginWindowTuple.getSerializedTuple(windowId = (int)t.getWindowId());
          break;

        case END_WINDOW:
          array = EndWindowTuple.getSerializedTuple(windowId = (int)t.getWindowId());
          break;

        case END_STREAM:
          array = EndStreamTuple.getSerializedTuple(windowId = (int)t.getWindowId());
          break;

        case RESET_WINDOW:
          com.malhartech.engine.ResetWindowTuple rwt = (com.malhartech.engine.ResetWindowTuple)t;
          array = ResetWindowTuple.getSerializedTuple(rwt.getBaseSeconds(), rwt.getIntervalMillis());
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
        write(DataTuple.getSerializedTuple(MessageType.CODEC_STATE_VALUE, dsp.state));
      }

      /*
       * Now that the state if any has been sent, we can proceed with the actual data we want to send.
       */
      array = PayloadTuple.getSerializedTuple(serde.getPartition(payload), dsp.data);
    }

    try {
      while (!write(array)) {
        sleep(5);
      }
    }
    catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  /**
   *
   * @param context
   */
  @Override
  public void activate(StreamContext context)
  {
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.attr(StreamContext.EVENT_LOOP).get();
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, this);

    logger.debug("registering publisher: {} {} windowId={} server={}", new Object[] {context.getSourceId(), context.getId(), context.getStartingWindowId(), context.getBufferServerAddress()});
    serde = context.attr(StreamContext.CODEC).get();
    super.activate(context.getStartingWindowId());
  }

  @Override
  public void deactivate()
  {
    eventloop.disconnect(this);
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

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    throw new RuntimeException("OutputStream is not supposed to receive anything!");
  }

  @Override
  public void setup(StreamContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerPublisher.class);
}

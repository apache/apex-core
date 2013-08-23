/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.stram.stream;

import static java.lang.Thread.sleep;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.codec.StatefulStreamCodec;
import com.datatorrent.stram.codec.StatefulStreamCodec.DataStatePair;
import com.datatorrent.stram.engine.ByteCounterStream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.tuple.Tuple;
import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.bufferserver.client.Publisher;
import com.datatorrent.bufferserver.packet.*;
import com.datatorrent.netlet.EventLoop;

/**
 * Implements tuple flow of node to then buffer server in a logical stream<p>
 * <br>
 * Extends SocketOutputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a write instance of a stream and hence would take care of persistence and retaining tuples till they are consumed<br>
 * Partitioning is managed by this instance of the buffer server<br>
 * <br>
 *
 * @since 0.3.2
 */
public class BufferServerPublisher extends Publisher implements ByteCounterStream
{
  private StreamCodec<Object> serde;
  private AtomicLong publishedByteCount = new AtomicLong(0);
  private EventLoop eventloop;
  private int count;
  private StatefulStreamCodec<Object> statefulSerde;

  public BufferServerPublisher(String sourceId, int queueCapacity)
  {
    super(sourceId, queueCapacity);
  }

  /**
   *
   * @param payload
   */
  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void put(Object payload)
  {
    count++;
    byte[] array;
    if (payload instanceof Tuple) {
      final Tuple t = (Tuple)payload;

      switch (t.getType()) {
        case CHECKPOINT:
          if (statefulSerde != null) {
            statefulSerde.resetState();
          }
          array = WindowIdTuple.getSerializedTuple((int)t.getWindowId());
          array[0] = MessageType.CHECKPOINT_VALUE;
          break;

        case BEGIN_WINDOW:
          array = BeginWindowTuple.getSerializedTuple((int)t.getWindowId());
          break;

        case END_WINDOW:
          array = EndWindowTuple.getSerializedTuple((int)t.getWindowId());
          break;

        case END_STREAM:
          array = EndStreamTuple.getSerializedTuple((int)t.getWindowId());
          break;

        case RESET_WINDOW:
          com.datatorrent.stram.tuple.ResetWindowTuple rwt = (com.datatorrent.stram.tuple.ResetWindowTuple)t;
          array = ResetWindowTuple.getSerializedTuple(rwt.getBaseSeconds(), rwt.getIntervalMillis());
          break;

        default:
          throw new UnsupportedOperationException("this data type is not handled in the stream");
      }
    }
    else {
      if (statefulSerde == null) {
        array = PayloadTuple.getSerializedTuple(serde.getPartition(payload), serde.toByteArray(payload));
      }
      else {
        DataStatePair dsp = statefulSerde.toDataStatePair(payload);
        /*
         * if there is any state write that for the subscriber before we write the data.
         */
        if (dsp.state != null) {
          write(DataTuple.getSerializedTuple(MessageType.CODEC_STATE_VALUE, dsp.state));
        }
        /*
         * Now that the state if any has been sent, we can proceed with the actual data we want to send.
         */
        array = PayloadTuple.getSerializedTuple(statefulSerde.getPartition(payload), dsp.data);
      }
    }

    try {
      while (!write(array)) {
        sleep(5);
      }
      publishedByteCount.addAndGet(array.length);
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

    logger.debug("registering publisher: {} {} windowId={} server={}", new Object[] {context.getSourceId(), context.getId(), context.getFinishedWindowId(), context.getBufferServerAddress()});
    serde = context.attr(StreamContext.CODEC).get();
    statefulSerde = serde instanceof StatefulStreamCodec ? (StatefulStreamCodec<Object>)serde : null;
    super.activate(null, context.getFinishedWindowId());
  }

  @Override
  public void deactivate()
  {
    eventloop.disconnect(this);
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

  @Override
  public long getByteCount(boolean reset)
  {
    if (reset) {
      return publishedByteCount.getAndSet(0);
    }

    return publishedByteCount.get();
  }

  @Override
  public void setSink(String id, Sink<Object> sink)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getCount(boolean reset)
  {
    try {
      return count;
    }
    finally {
      if (reset) {
        count = 0;
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerPublisher.class);
}

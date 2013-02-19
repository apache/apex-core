/**
 * Copyright (c) 2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.engine.*;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implement tuple flow from buffer server to the node in a logical stream<p>
 * <br>
 * Extends SocketInputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a read instance of a stream and takes care of connectivity with upstream buffer server<br>
 */
public class BufferServerInputStream extends SocketInputStream<Message>
{
  private static final Logger logger = LoggerFactory.getLogger(BufferServerInputStream.class);
  private final HashMap<String, Sink<Object>> outputs = new HashMap<String, Sink<Object>>();
  private long baseSeconds;
  private int lastWindowId = WindowGenerator.MAX_WINDOW_ID;
  @SuppressWarnings("VolatileArrayField")
  private volatile Sink<Object>[] sinks = NO_SINKS;
  private final StreamCodec<Object> serde;
  DataStatePair dsp = new DataStatePair();
  private int mask;
  private Collection<Integer> partitions;

  public BufferServerInputStream(StreamCodec<Object> serde)
  {
    this.serde = serde;
  }

  @Override
  public void activate(StreamContext context)
  {
    super.activate(context);
    activateSinks();

    mask = context.getPartitionMask();
    partitions = context.getPartitions();

    baseSeconds = context.getStartingWindowId() & 0xffffffff00000000L;
    logger.debug("registering subscriber: id={} upstreamId={} streamLogicalName={} windowId={} mask={} partitions={} server={}", new Object[] {context.getSinkId(), context.getSourceId(), context.getId(), context.getStartingWindowId(), context.getPartitionMask(), context.getPartitions(), context.getBufferServerAddress()});
    ClientHandler.subscribe(channel,
                            context.getSinkId(),
                            context.getId() + '/' + context.getSinkId(),
                            context.getSourceId(),
                            context.getPartitionMask(),
                            context.getPartitions(),
                            context.getStartingWindowId());
  }

  @Override
  public void messageReceived(io.netty.channel.ChannelHandlerContext ctx, Message data) throws Exception
  {
    Tuple t;
    switch (data.getType()) {
      case CHECKPOINT:
        serde.resetState();
        return;

      case CODEC_STATE:
        dsp.state = data.getCodecState().getData().toByteArray();
        logger.debug("received codec state {}", dsp.state);
        return;

      case PAYLOAD:
        dsp.data = data.getPayload().getData().toByteArray();
        if (mask != 0) {
          assert (data.getPayload().hasPartition());
          assert (partitions.contains(data.getPayload().getPartition() & mask));
        }
        Object o = serde.fromByteArray(dsp);
        for (Sink<Object> s: sinks) {
          s.process(o);
        }
        return;

      case END_WINDOW:
        t = new EndWindowTuple();
        t.setWindowId(baseSeconds | (lastWindowId = data.getEndWindow().getWindowId()));
        break;

      case END_STREAM:
        t = new EndStreamTuple();
        t.setWindowId(baseSeconds | data.getEndStream().getWindowId());
        break;

      case RESET_WINDOW:
        baseSeconds = (long)data.getResetWindow().getBaseSeconds() << 32;
        if (lastWindowId < WindowGenerator.MAX_WINDOW_ID) {
          return;
        }
        t = new ResetWindowTuple();
        t.setWindowId(baseSeconds | data.getResetWindow().getWidth());
        break;

      case BEGIN_WINDOW:
        t = new Tuple(data.getType());
        t.setWindowId(baseSeconds | data.getBeginWindow().getWindowId());
        break;

      default:
        throw new IllegalArgumentException("Unhandled Message Type " + data.getType());
    }

    for (int i = sinks.length; i-- > 0;) {
      sinks[i].process(t);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setSink(String id, Sink<Message> sink)
  {
    if (sink == null) {
      outputs.remove(id);
      if (outputs.isEmpty()) {
        sinks = NO_SINKS;
      }
    }
    else {
      outputs.put(id, (Sink)sink);
      if (sinks != NO_SINKS) {
        activateSinks();
      }
    }

  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }

  private void activateSinks()
  {
    @SuppressWarnings("unchecked")
    Sink<Object>[] newSinks = (Sink<Object>[])Array.newInstance(Sink.class, outputs.size());
    int i = 0;
    for (final Sink<Object> s: outputs.values()) {
      newSinks[i++] = s;
    }
    sinks = newSinks;
  }

  @Override
  public void process(Message tuple)
  {
    throw new IllegalAccessError("Attempt to pass payload " + tuple + " to " + this + " from source other than buffer server!");
  }

}

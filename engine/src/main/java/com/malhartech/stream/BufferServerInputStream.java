/**
 * Copyright (c) 2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.engine.*;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implement tuple flow from buffer server to the node in a logical stream<p>
 * <br>
 * Extends SocketInputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a read instance of a stream and takes care of connectivity with upstream buffer server<br>
 */
public class BufferServerInputStream extends SocketInputStream<Buffer.Data>
{
  private static final Logger logger = LoggerFactory.getLogger(BufferServerInputStream.class);
  private final HashMap<String, Sink> outputs = new HashMap<String, Sink>();
  private long baseSeconds = 0;
  @SuppressWarnings("VolatileArrayField")
  private volatile Sink[] sinks = NO_SINKS;
  private final StreamCodec serde;
  DataStatePair dsp = new DataStatePair();

  public BufferServerInputStream(StreamCodec serde)
  {
    this.serde = serde;
  }

  @Override
  public void activate(StreamContext context)
  {
    super.activate(context);
    activateSinks();

    String type = "unused";
    logger.debug("registering subscriber: id={} upstreamId={} streamLogicalName={} windowId={}", new Object[] {context.getSinkId(), context.getSourceId(), context.getId(), context.getStartingWindowId()});
    ClientHandler.subscribe(channel,
                            context.getSinkId(),
                            context.getId() + '/' + context.getSinkId(),
                            context.getSourceId(), type,
                            context.getPartitions(), context.getStartingWindowId());
  }

  @Override
  public void messageReceived(io.netty.channel.ChannelHandlerContext ctx, Data data) throws Exception
  {
    Tuple t;
    switch (data.getType()) {
      case CODEC_STATE:
        dsp.state = data.getCodecState().getData().toByteArray();
        return;

      case SIMPLE_DATA:
        dsp.data = data.getSimpleData().getData().toByteArray();
        Object o = serde.fromByteArray(dsp);
        for (Sink s: sinks) {
          s.process(o);
        }
        return;

      case PARTITIONED_DATA:
        dsp.data = data.getPartitionedData().getData().toByteArray();
        o = serde.fromByteArray(dsp);
        for (Sink s: sinks) {
          s.process(o);
        }
        return;

      case END_WINDOW:
        t = new EndWindowTuple();
        t.setWindowId(baseSeconds | data.getWindowId());
        break;

      case END_STREAM:
        t = new EndStreamTuple();
        t.setWindowId(baseSeconds | data.getWindowId());
        break;

      case RESET_WINDOW:
        t = new ResetWindowTuple();
        baseSeconds = (long)data.getWindowId() << 32;
        t.setWindowId(baseSeconds | data.getResetWindow().getWidth());
        break;

      default:
        t = new Tuple(data.getType());
        t.setWindowId(baseSeconds | data.getWindowId());
        break;
    }

    for (int i = sinks.length; i-- > 0;) {
      sinks[i].process(t);
    }
  }

  @Override
  public Sink setSink(String id, Sink sink)
  {
    if (sink == null) {
      sink = outputs.remove(id);
      if (outputs.isEmpty()) {
        sinks = NO_SINKS;
      }
    }
    else {
      sink = outputs.put(id, sink);
      if (sinks != NO_SINKS) {
        activateSinks();
      }
    }

    return sink;
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }

  @SuppressWarnings("SillyAssignment")
  private void activateSinks()
  {
    sinks = new Sink[outputs.size()];
    int i = 0;
    for (final Sink s: outputs.values()) {
      sinks[i++] = s;
    }
    sinks = sinks;
  }

  @Override
  public void process(Data tuple)
  {
    throw new IllegalAccessError("Attempt to pass payload " + tuple + " to " + this + " from source other than buffer server!");
  }
}

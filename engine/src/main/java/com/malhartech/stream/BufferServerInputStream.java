/**
 * Copyright (c) 2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.dag.*;
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
  private static Logger logger = LoggerFactory.getLogger(BufferServerInputStream.class);
  private HashMap<String, Sink> outputs = new HashMap<String, Sink>();
  private long baseSeconds = 0;
  private Iterable<Sink> sinks;
  private SerDe serde;

  @Override
  public void activate(StreamContext context)
  {
    super.activate(context);
    serde = context.getSerDe();

    sinks = outputs.values();
    String type = "paramNotRequired?"; // TODO: why do we need this?
    logger.debug("registering subscriber: id={} upstreamId={} streamLogicalName={}", new Object[] {context.getSinkId(), context.getSourceId(), context.getId()});
    ClientHandler.registerPartitions(channel, context.getSinkId(), context.getId() + '/' + context.getSinkId(), context.getSourceId(), type, ((BufferServerStreamContext)context).getPartitions(), context.getStartingWindowId());
  }

  @Override
  public void messageReceived(io.netty.channel.ChannelHandlerContext ctx, Data data) throws Exception
  {
//    StreamContext context = ctx.channel().attr(CONTEXT).get();
//    if (serde == null) {
//      logger.warn("serde is not setup for the InputSocketStream");
//    }
//    else {
      Tuple t;
      switch (data.getType()) {
        case SIMPLE_DATA:
          Object o = serde.fromByteArray(data.getSimpleData().getData().toByteArray());
          for (Sink s: sinks) {
            s.process(o);
          }
          return;

        case PARTITIONED_DATA:
          o = serde.fromByteArray(data.getPartitionedData().getData().toByteArray());
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

      for (Sink s: sinks) {
        s.process(t);
      }
//    }
  }

  @Override
  public Sink connect(String id, Sink sink)
  {
    outputs.put(id, sink);
    return null;
  }

  @Override
  public final void process(Object payload)
  {
    throw new IllegalAccessError("Attempt to pass payload from source other than buffer server!");
  }
}

/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.dag.*;
import com.malhartech.netty.ClientPipelineFactory;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 *
 * @author chetan
 */
public class SocketInputStream
  extends SimpleChannelUpstreamHandler
  implements Stream
{
  private static final Logger logger =
                              Logger.getLogger(ClientHandler.class.getName());
  public static final ChannelLocal<StreamContext> contexts = new ChannelLocal<StreamContext>()
  {
    @Override
    protected StreamContext initialValue(Channel channel)
    {
      return null;
    }
  };
  protected Channel channel;
  private ClientBootstrap bootstrap;
  private long tupleCount;

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    StreamContext context = contexts.get(ctx.getChannel());
    if (context == null) {
      logger.log(Level.WARNING, "Context is not setup for the InputSocketStream");
    }
    else {
      Data d = (Data) e.getMessage();

      Tuple t;
      switch (d.getType()) {
        case SIMPLE_DATA:
          tupleCount++;
          t = new Tuple(context.getSerDe().fromByteArray(d.getSimpledata().
            getData().toByteArray()));
          t.setType(DataType.SIMPLE_DATA);
          break;

        case PARTITIONED_DATA:
          tupleCount++;
          t = new Tuple(context.getSerDe().fromByteArray(d.getPartitioneddata().
            getData().toByteArray()));
          /*
           * we really do not distinguish between SIMPLE_DATA and
           * PARTITIONED_DATA
           */
          t.setType(DataType.SIMPLE_DATA);
          break;

        case END_WINDOW:
          t = new EndWindowTuple();
          ((EndWindowTuple) t).setTupleCount(tupleCount);
          break;

        case BEGIN_WINDOW:
          tupleCount = 0;
          
        default:
          t = new Tuple(null);
          t.setType(d.getType());
          break;
      }

      t.setWindowId(d.getWindowId());
      t.setContext(context);
      context.sink(t);
    }
  }

  protected ClientPipelineFactory getClientPipelineFactory()
  {
    return new ClientPipelineFactory(SocketInputStream.class);
  }

  @Override
  public void setup(StreamConfiguration config)
  {
    bootstrap = new ClientBootstrap(
      new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool()));

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(getClientPipelineFactory());

    // Make a new connection.
    ChannelFuture future = bootstrap.connect(config.getBufferServerAddress());
    channel = future.awaitUninterruptibly().getChannel();
  }

  @Override
  public void setContext(com.malhartech.dag.StreamContext context)
  {
    contexts.set(channel, context);
  }

  @Override
  public void teardown()
  {
    contexts.remove(channel);
    channel.close();
    channel.getCloseFuture().awaitUninterruptibly();
    bootstrap.releaseExternalResources();
  }

  @Override
  public StreamContext getContext()
  {
    return contexts.get(channel);
  }
}

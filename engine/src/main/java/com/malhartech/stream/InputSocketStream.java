/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.dag.Tuple;
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
public class InputSocketStream extends SimpleChannelUpstreamHandler implements Stream
{

  private static final Logger logger =
                              Logger.getLogger(ClientHandler.class.getName());
  private ClientBootstrap bootstrap;
  public static final ChannelLocal<StreamContext> contexts = new ChannelLocal<StreamContext>()
  {

    @Override
    protected StreamContext initialValue(Channel channel)
    {
      return null;
    }
  };
  protected Channel channel;

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    StreamContext context = contexts.get(ctx.getChannel());
    if (context == null) {
      logger.log(Level.WARNING, "Context is not setup for the InputSocketStream");
    }
    else {
      Data d = (Data) e.getMessage();
      logger.log(Level.INFO, "received {0} with windowid {1}", new Object[]{d.getType(), d.getWindowId()});

      Object o;
      if (d.getType() == Data.DataType.SIMPLE_DATA) {
        o = context.getSerDe().fromByteArray(d.getSimpledata().getData().toByteArray());
      }
      else if (d.getType() == Data.DataType.PARTITIONED_DATA) {
        o = context.getSerDe().fromByteArray(d.getPartitioneddata().getData().toByteArray());
      }
      else {
        o = null;
      }

      Tuple t = new Tuple(o);
      t.setContext(context);
      t.setData(d);
      context.getSink().doSomething(t);
    }
  }

  protected ClientPipelineFactory getClientPipelineFactory()
  {
    return new ClientPipelineFactory(InputSocketStream.class);
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
}

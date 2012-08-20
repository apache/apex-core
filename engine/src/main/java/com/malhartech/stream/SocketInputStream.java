/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.bufferserver.netty.ClientPipelineFactory;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.DirectChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelLocal;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author chetan
 */
public class SocketInputStream extends SimpleChannelUpstreamHandler implements Stream
{
  private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);
  protected static final ChannelLocal<StreamContext> contexts = new ChannelLocal<StreamContext>()
  {
    @Override
    protected StreamContext initialValue(Channel channel)
    {
      return null;
    }
  };
  protected Channel channel;
  private ClientBootstrap bootstrap;
  private InetSocketAddress serverAddress;
  private StreamContext context;

  protected ClientPipelineFactory getClientPipelineFactory()
  {
    return new ClientPipelineFactory(this.getClass());
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

    serverAddress = config.getBufferServerAddress();
  }

  @Override
  public void setContext(com.malhartech.dag.StreamContext context)
  {
    this.context = context;
//    contexts.set(channel, context);
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
    return context;
    //return contexts.get(channel); // results in NPE when called prior to activate
  }

  @Override
  public void activate()
  {
    // Make a new connection.
    ChannelFuture future = bootstrap.connect(serverAddress);
    channel = future.awaitUninterruptibly().getChannel();

    // Netty does not provide a way to read in all the data that comes
    // onto the channel into a byte buffer managed by the user. It causes
    // various problems:
    // 1. There is excessive copy of data between the 2 buffers.
    // 2. Once the BufferFactory has given out the buffer, it does not know
    //    if it can ever recycle it.
    // 3. Causes fragmentation and need for garbage collection
    
    // Netty needs some way to prevent it.
    
//    channel.getConfig().setBufferFactory(new ChannelBufferFactory() {
//      @SuppressWarnings("PackageVisibleField")
//      DirectChannelBufferFactory impl = new DirectChannelBufferFactory(1024 * 1024);
//
//      @Override
//      public ChannelBuffer getBuffer(int i)
//      {
//        logger.debug(String.valueOf(i));
//        return impl.getBuffer(i);
//      }
//
//      @Override
//      public ChannelBuffer getBuffer(ByteOrder bo, int i)
//      {
//        logger.debug(bo + ", " + i);
//        return impl.getBuffer(bo, i);
//      }
//
//      @Override
//      public ChannelBuffer getBuffer(byte[] bytes, int i, int i1)
//      {
//        logger.debug(bytes + ", " + i + " " + i1);
//        return impl.getBuffer(bytes, i, i1);
//      }
//
//      @Override
//      public ChannelBuffer getBuffer(ByteOrder bo, byte[] bytes, int i, int i1)
//      {
//        logger.debug(bo + ", " + bytes + ", " + i + " " + i1);
//        return impl.getBuffer(bytes, i, i1);
//      }
//
//      @Override
//      public ChannelBuffer getBuffer(ByteBuffer bb)
//      {
//        logger.debug("" + bb);
//        return impl.getBuffer(bb);
//      }
//
//      @Override
//      public ByteOrder getDefaultOrder()
//      {
//        ByteOrder bo = impl.getDefaultOrder();
//        logger.debug("" + bo);
//        return bo;
//      }
//    });
    contexts.set(channel, context);
  }
}

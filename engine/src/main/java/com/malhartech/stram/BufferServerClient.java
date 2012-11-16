/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.bufferserver.netty.ClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates buffer server control interface, used by the master for purging data.
 */
@Sharable
class BufferServerClient extends ChannelInboundMessageHandlerAdapter<Object> {
  private final static Logger LOG = LoggerFactory.getLogger(BufferServerClient.class);

  /**
   * Use a single thread group for all buffer server interactions.
   */
  final static NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
  final Bootstrap bootstrap = new Bootstrap();
  final InetSocketAddress addr;

  BufferServerClient(InetSocketAddress addr) {
    this.addr = addr;
    bootstrap.group(eventLoopGroup)
    .channel(NioSocketChannel.class)
    .remoteAddress(addr)
    .handler(new ClientInitializer(this));
  }

  void purge(String sourceIdentifier, long windowId) {
    LOG.debug("Purging sourceId=" + sourceIdentifier + ", windowId=" + windowId + " @" + addr);
    Channel channel = bootstrap.connect().syncUninterruptibly().channel();
    ClientHandler.purge(channel, sourceIdentifier, windowId);
  }

  void reset(String sourceIdentifier, long windowId) {
    LOG.debug("Reset sourceId=" + sourceIdentifier + ", windowId=" + windowId + " @" + addr);
    Channel channel = bootstrap.connect().syncUninterruptibly().channel();
    ClientHandler.reset(channel, sourceIdentifier, windowId);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, Object msg) throws Exception {
    LOG.debug("messageReceived: " + msg);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.error("Buffer server request failed", cause);
    ctx.close();
  }

  @Override
  public void endMessageReceived(ChannelHandlerContext ctx) throws Exception {
    super.endMessageReceived(ctx);
    //LOG.debug("endMessageReceived");
    ctx.close();
  }

  @Override
  public void beginMessageReceived(ChannelHandlerContext ctx) throws Exception {
    //LOG.debug("beginMessageReceived");
  }

}
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.netty.ClientPipelineFactory;
import com.malhartech.stram.StreamContext;
import java.net.InetSocketAddress;
import java.security.InvalidParameterException;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 *
 * @author chetan
 */
public class InputSocketStream extends SimpleChannelUpstreamHandler implements Stream
{
  private com.malhartech.dag.StreamContext context;
  private ClientBootstrap bootstrap;
  private ChannelFuture future;

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    Data d = (Data) e.getMessage();
    Object o;
    if (d.getType() == Data.DataType.SIMPLE_DATA) {
      o = context.getSerDe().fromByteArray(d.getSimpledata().toByteArray());
    }
    else if (d.getType() == Data.DataType.PARTITIONED_DATA) {
      o = context.getSerDe().fromByteArray(d.getPartitioneddata().toByteArray());
    }
    else {
      o = null;
    }

    Tuple t = new Tuple(o);
    t.setContext(context);
    t.setData(d);
    context.getSink().doSomething(t);
  }
  
  public void setup(StreamConfiguration config)
  {
    bootstrap = new ClientBootstrap(
            new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(new ClientPipelineFactory(InputSocketStream.class));

    // Make a new connection.
    future = bootstrap.connect(config.getSourceSocketAddress());
    future.awaitUninterruptibly();
  }

  public void process(com.malhartech.dag.StreamContext context)
  {
    this.context = context;
    // send the subscribe request.
  }

  public void teardown(StreamConfiguration config)
  {
    future.getChannel().close();
    future.getChannel().getCloseFuture().awaitUninterruptibly();
    bootstrap.releaseExternalResources();
  }
}

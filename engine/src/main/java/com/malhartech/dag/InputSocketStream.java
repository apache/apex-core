package com.malhartech.dag;

import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.netty.ClientPipelineFactory;

/**
 *
 * @author chetan
 */
public class InputSocketStream extends SimpleChannelUpstreamHandler implements Stream
{
  private com.malhartech.dag.StreamContext context;
  private ClientBootstrap bootstrap;
  protected ChannelFuture future;

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
  
  public ClientPipelineFactory getClientPipelineFactory() {
    return new ClientPipelineFactory(InputSocketStream.class);    
  }
  
  public void setup(StreamConfiguration config)
  {
    bootstrap = new ClientBootstrap(
            new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(getClientPipelineFactory());

    // Make a new connection.
    future = bootstrap.connect(config.getBufferServerAddress());
    future.awaitUninterruptibly();
  }

  public void setContext(com.malhartech.dag.StreamContext context)
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

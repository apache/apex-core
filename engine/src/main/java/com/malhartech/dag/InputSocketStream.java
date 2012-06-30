package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.netty.ClientPipelineFactory;
import com.malhartech.stram.StreamContext;
import java.net.InetSocketAddress;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */
/**
 *
 * @author chetan
 */
public class InputSocketStream extends SimpleChannelUpstreamHandler
{

  final StreamContext context;
  final Collection<Tuple> collection;
  SerDe serde;

  InputSocketStream(StreamContext context, Collection<Tuple> collection)
  {
    this.context = context;
    if (context.getBufferServerHost() == null
        || context.getBufferServerPort() == 0) {
      throw new InvalidParameterException("null host or port passed for socket stream context");
    }
    else {
      // do addtional validation
    }

    this.collection = collection;
  }

  public void setSerDe(SerDe serde)
  {
    this.serde = serde;
  }

  void connect()
  {
    ClientBootstrap bootstrap = new ClientBootstrap(
            new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(new ClientPipelineFactory(InputSocketStream.class));

    // Make a new connection.
    bootstrap.connect(new InetSocketAddress(context.getBufferServerHost(),
                                            Integer.valueOf(context.getBufferServerPort())));


    // send the subscribe request.
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    Data d = (Data) e.getMessage();
    Object o;
    if (d.getType() == Data.DataType.SIMPLE_DATA) {
      o = serde.fromByteArray(d.getSimpledata().toByteArray());
    }
    else if (d.getType() == Data.DataType.PARTITIONED_DATA) {
      o = serde.fromByteArray(d.getPartitioneddata().toByteArray());
    }
    else {
      o = null;
    }

    Tuple t = new Tuple(o, context);
    t.setData(d);
    synchronized (collection) {
      collection.add(t);
      collection.notify();
    }
  }
}

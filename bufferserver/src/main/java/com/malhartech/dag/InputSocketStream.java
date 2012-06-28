package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.netty.ClientPipelineFactory;
import java.net.InetSocketAddress;
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

  String host;
  int port;
  final Collection<Tuple> queue;
  SerDe serde;

  InputSocketStream(String host, int port, Collection<Tuple> queue)
  {
    this.host = host;
    this.port = port;
    this.queue = queue;
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
    bootstrap.connect(new InetSocketAddress(host, port));


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

    Tuple t = new Tuple(o, this);
    t.setData(d);
    synchronized (queue) {
      queue.add(t);
      queue.notify();
    }
  }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.PartitionedData;
import com.malhartech.bufferserver.Buffer.SimpleData;
import com.malhartech.netty.ClientPipelineFactory;

/**
 *
 * @author chetan
 */
public class OutputSocketStream extends SimpleChannelDownstreamHandler implements Sink, Stream
{

  private StreamContext context;
  private ClientBootstrap bootstrap;
  protected Channel channel;


  public void doSomething(Tuple t)
  {
    Data.Builder db = Data.newBuilder();
    db.setWindowId(t.getData().getWindowId());

    byte partition[] = context.getSerDe().getPartition(t.object);
    if (partition == null) {
      SimpleData.Builder sdb = SimpleData.newBuilder();
      sdb.setData(ByteString.copyFrom(context.getSerDe().toByteArray(t.object)));

      db.setType(Data.DataType.SIMPLE_DATA);
      db.setSimpledata(sdb);
    }
    else {
      PartitionedData.Builder pdb = PartitionedData.newBuilder();
      pdb.setPartition(ByteString.copyFrom(partition));
      pdb.setData(ByteString.copyFrom(context.getSerDe().toByteArray(t.object)));

      db.setType(Data.DataType.PARTITIONED_DATA);
      db.setPartitioneddata(pdb);
    }

    channel.write(db.build());
  }

  public ClientPipelineFactory getClientPipelineFactory() {
    return new ClientPipelineFactory(OutputSocketStream.class);    
  }  
  
  public void setup(StreamConfiguration config)
  {
    bootstrap = new ClientBootstrap(
            new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                              Executors.newCachedThreadPool()));

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(getClientPipelineFactory());

    // Make a new connection.
    ChannelFuture future = bootstrap.connect(config.getBufferServerAddress());
    future.awaitUninterruptibly();
    channel = future.getChannel();
  }

  public void setContext(com.malhartech.dag.StreamContext context)
  {
    this.context = context;
    // send publisher request
  }

  public void teardown(StreamConfiguration config)
  {
    channel.close();
    channel.getCloseFuture().awaitUninterruptibly();
    bootstrap.releaseExternalResources();
  }
}

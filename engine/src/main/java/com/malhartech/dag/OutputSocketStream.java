/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.Builder;
import com.malhartech.bufferserver.Buffer.PartitionedData;
import com.malhartech.bufferserver.Buffer.SimpleData;
import com.malhartech.netty.ClientPipelineFactory;
import com.malhartech.stram.StreamContext;
import java.net.InetSocketAddress;
import java.nio.channels.NotYetConnectedException;
import java.security.InvalidParameterException;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 *
 * @author chetan
 */
public class OutputSocketStream extends SimpleChannelDownstreamHandler implements Sink
{

  private final StreamContext context;
  private final SerDe serde;
  private Channel channel = null;

  public OutputSocketStream(StreamContext context, SerDe serde)
  {
    this.context = context;
    if (this.context.getBufferServerHost() == null
        || this.context.getBufferServerPort() == null) {
      throw new InvalidParameterException("null host or port passed for socket stream context!");
    }

    this.serde = serde;
  }
  
  public StreamContext getStreamContext()
  {
    return this.context;
  }

  void connect()
  {
    ClientBootstrap bootstrap = new ClientBootstrap(
            new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                              Executors.newCachedThreadPool()));

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(new ClientPipelineFactory(OutputSocketStream.class));

    // Make a new connection.
    ChannelFuture cf = bootstrap.connect(new InetSocketAddress(context.getBufferServerHost(),
                                                               Integer.valueOf(context.getBufferServerPort())));
    cf.addListener(new ChannelFutureListener()
    {

      public void operationComplete(ChannelFuture cf) throws Exception
      {
        channel = cf.getChannel();
        sendPublishRequest();
      }
    });
  }
  
  private void sendPublishRequest()
  {
    // TBI
  }

  public void doSomething(Tuple t)
  {
    if (channel == null) {
      throw new NotYetConnectedException();
    }
    
    Data.Builder db = Data.newBuilder();
      db.setWindowId(t.data.getWindowId());
    
    byte partition[] = serde.getPartition(t.object);
    if (partition == null) {
      SimpleData.Builder sdb = SimpleData.newBuilder();
      sdb.setData(ByteString.copyFrom(serde.toByteArray(t.object)));

      db.setType(Data.DataType.SIMPLE_DATA);
      db.setSimpledata(sdb);
    }
    else {
      PartitionedData.Builder pdb = PartitionedData.newBuilder();
      pdb.setPartition(ByteString.copyFrom(partition));
      pdb.setData(ByteString.copyFrom(serde.toByteArray(t.object)));

      db.setType(Data.DataType.PARTITIONED_DATA);
      db.setPartitioneddata(pdb);
    }
    
    channel.write(db.build());
  }

}

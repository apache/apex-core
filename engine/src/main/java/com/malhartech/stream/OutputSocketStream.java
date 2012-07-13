/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.stream;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.PartitionedData;
import com.malhartech.bufferserver.Buffer.SimpleData;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.dag.Tuple;
import com.malhartech.netty.ClientPipelineFactory;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

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
    Data data = t.getData();

    switch (data.getType()) {
      case BEGIN_WINDOW:
        break;

      case END_WINDOW:
        break;


      case SIMPLE_DATA:
      case PARTITIONED_DATA:
        Data.Builder db = Data.newBuilder();
        db.setWindowId(t.getData().getWindowId());

        byte partition[] = context.getSerDe().getPartition(t.getObject());
        if (partition == null) {
          SimpleData.Builder sdb = SimpleData.newBuilder();
          sdb.setData(ByteString.copyFrom(context.getSerDe().toByteArray(t.getObject())));

          db.setType(Data.DataType.SIMPLE_DATA);
          db.setSimpledata(sdb);
        }
        else {
          PartitionedData.Builder pdb = PartitionedData.newBuilder();
          pdb.setPartition(ByteString.copyFrom(partition));
          pdb.setData(ByteString.copyFrom(context.getSerDe().toByteArray(t.getObject())));

          db.setType(Data.DataType.PARTITIONED_DATA);
          db.setPartitioneddata(pdb);
        }
        
        data = db.build();
        break;

      default:
        throw new UnsupportedOperationException("this data type is not handled in the stream");
    }

    channel.write(data);
  }

  protected ClientPipelineFactory getClientPipelineFactory()
  {
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

  public void teardown()
  {
    channel.close();
    channel.getCloseFuture().awaitUninterruptibly();
    bootstrap.releaseExternalResources();
  }
}

/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.stream;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.Buffer.BeginWindow;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.EndWindow;
import com.malhartech.bufferserver.Buffer.PartitionedData;
import com.malhartech.bufferserver.Buffer.SimpleData;
import com.malhartech.dag.*;
import com.malhartech.netty.ClientPipelineFactory;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author chetan
 */
public class SocketOutputStream extends SimpleChannelDownstreamHandler
  implements Sink, Stream
{
  private static Logger logger = LoggerFactory.getLogger(SocketOutputStream.class);
  private StreamContext context;
  protected ClientBootstrap bootstrap;
  protected Channel channel;

  public void doSomething(Tuple t)
  {
    Data.Builder db = Data.newBuilder();
    db.setType(t.getType());
    db.setWindowId(t.getWindowId());
    
    switch (t.getType()) {
      case BEGIN_WINDOW:
        BeginWindow.Builder bw = BeginWindow.newBuilder();
        bw.setNode("SOS");

        db.setBeginwindow(bw);
        break;

      case END_WINDOW:
        EndWindow.Builder ew = EndWindow.newBuilder();
        ew.setNode("SOS");
        ew.setTupleCount(((EndWindowTuple) t).getTupleCount());

        db.setEndwindow(ew);
        break;

      case PARTITIONED_DATA:
        logger.info("got partitioned data " + t.getObject());
      case SIMPLE_DATA:

        byte partition[] = context.getSerDe().getPartition(t.getObject());
        if (partition == null) {
          SimpleData.Builder sdb = SimpleData.newBuilder();
          sdb.setData(ByteString.copyFrom(context.getSerDe().toByteArray(t.
            getObject())));

          db.setType(Data.DataType.SIMPLE_DATA);
          db.setSimpledata(sdb);
        }
        else {
          PartitionedData.Builder pdb = PartitionedData.newBuilder();
          pdb.setPartition(ByteString.copyFrom(partition));
          pdb.setData(ByteString.copyFrom(context.getSerDe().toByteArray(t.
            getObject())));

          db.setType(Data.DataType.PARTITIONED_DATA);
          db.setPartitioneddata(pdb);
        }
        break;

      default:
        throw new UnsupportedOperationException("this data type is not handled in the stream");
    }

    channel.write(db.build());
  }

  protected ClientPipelineFactory getClientPipelineFactory()
  {
    return new ClientPipelineFactory(SocketOutputStream.class);
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
    channel = future.awaitUninterruptibly().getChannel();
  }

  public void setContext(com.malhartech.dag.StreamContext context)
  {
    this.context = context;
    // send publisher request
  }

  public StreamContext getContext()
  {
    return context;
  }

  public void teardown()
  {
    channel.close();
    channel.getCloseFuture().awaitUninterruptibly();
    bootstrap.releaseExternalResources();
  }
}

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
import java.security.InvalidParameterException;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 *
 * @author chetan
 */
public class OutputSocketStream extends SimpleChannelDownstreamHandler
{

  final StreamContext context;
  SerDe serde;

  public OutputSocketStream(StreamContext context)
  {
    this.context = context;
    if (this.context.getBufferServerHost() == null
        || this.context.getBufferServerPort() == null) {
      throw new InvalidParameterException("null host or port passed for socket stream context!");
    }
  }

  void connect()
  {
    ClientBootstrap bootstrap = new ClientBootstrap(
            new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                              Executors.newCachedThreadPool()));

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(new ClientPipelineFactory(OutputSocketStream.class));

    // Make a new connection.
    bootstrap.connect(new InetSocketAddress(context.getBufferServerHost(),
                                            Integer.valueOf(context.getBufferServerPort())));


    // send the publish request.
  }

  void send(Tuple t, byte[] partition)
  {
    Data.Builder db = Data.newBuilder();
    if (partition == null) {
      SimpleData.Builder sdb = SimpleData.newBuilder();
      sdb.setData(ByteString.copyFrom(serde.toByteArray(t.object)));

      db.setType(Data.DataType.SIMPLE_DATA);
      db.setSimpledata(sdb);
      db.setWindowId(t.data.getWindowId());
    }
    else {
      PartitionedData.Builder pdb = PartitionedData.newBuilder();
      pdb.setPartition(ByteString.copyFrom(partition));
      pdb.setData(ByteString.copyFrom(serde.toByteArray(t.object)));
      
      db.setType(Data.DataType.PARTITIONED_DATA);
      db.setPartitioneddata(pdb);
    }

  }
}

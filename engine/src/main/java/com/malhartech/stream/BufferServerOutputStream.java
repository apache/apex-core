/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.dag.EndWindowTuple;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferServerOutputStream extends SocketOutputStream implements Sink
{
  private static Logger logger = LoggerFactory.getLogger(BufferServerOutputStream.class);

  @Override
  public void doSomething(Tuple t)
  {
    Buffer.Data.Builder db = Buffer.Data.newBuilder();
    db.setType(t.getType());
    db.setWindowId(t.getWindowId());

    switch (t.getType()) {
      case BEGIN_WINDOW:
        Buffer.BeginWindow.Builder bw = Buffer.BeginWindow.newBuilder();
        bw.setNode("SOS");

        db.setBeginWindow(bw);
        break;

      case END_WINDOW:
        Buffer.EndWindow.Builder ew = Buffer.EndWindow.newBuilder();
        ew.setNode("SOS");
        ew.setTupleCount(((EndWindowTuple) t).getTupleCount());

        db.setEndWindow(ew);
        break;

      case END_STREAM:
        break;
        
      case PARTITIONED_DATA:
        logger.warn("got partitioned data " + t.getObject());
      case SIMPLE_DATA:

        byte partition[] = context.getSerDe().getPartition(t.getObject());
        if (partition == null) {
          Buffer.SimpleData.Builder sdb = Buffer.SimpleData.newBuilder();
          sdb.setData(ByteString.copyFrom(context.getSerDe().toByteArray(t.getObject())));

          db.setType(Buffer.Data.DataType.SIMPLE_DATA);
          db.setSimpleData(sdb);
        }
        else {
          Buffer.PartitionedData.Builder pdb = Buffer.PartitionedData.newBuilder();
          pdb.setPartition(ByteString.copyFrom(partition));
          pdb.setData(ByteString.copyFrom(context.getSerDe().toByteArray(t.getObject())));

          db.setType(Buffer.Data.DataType.PARTITIONED_DATA);
          db.setPartitionedData(pdb);
        }
        break;

      default:
        throw new UnsupportedOperationException("this data type is not handled in the stream");
    }

//    logger.debug("channel write with data {}" + db.build());
    channel.write(db.build());
  }

  @Override
  public void activate()
  {
    super.activate();
    
    BufferServerStreamContext sc = (BufferServerStreamContext)getContext();
    logger.debug("registering publisher: {} {}", sc.getSourceId(), sc.getId());
    ClientHandler.publish(channel, sc.getSourceId(), sc.getId());    
  }
}
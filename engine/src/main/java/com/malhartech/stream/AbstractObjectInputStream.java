/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.BeginWindow;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.EndWindow;
import com.malhartech.bufferserver.Buffer.PartitionedData;
import com.malhartech.bufferserver.Buffer.SimpleData;
import com.malhartech.dag.InputAdapter;
import com.malhartech.dag.SerDe;
import com.malhartech.dag.StreamContext;
import com.malhartech.dag.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractObjectInputStream implements InputAdapter
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractObjectInputStream.class);
  protected StreamContext context = null;
  protected volatile long tupleCount = 0;
  protected volatile long timemillis = 0;

  public void setContext(StreamContext context)
  {
    this.context = context;
  }
  
  public StreamContext getContext()
  {
    return this.context;
  }

  public abstract Object getObject(Object object);

  public void sendTuple(Object o)
  {
    SerDe serde = context.getSerDe();
    byte[] partition = serde.getPartition(o);

    Data.Builder db = Data.newBuilder();
    if (partition == null) {
      SimpleData.Builder sdb = SimpleData.newBuilder();

      /*
       * we dont care about byte array
       */
      sdb.setData(ByteString.EMPTY);
      db.setType(Data.DataType.SIMPLE_DATA);
      db.setSimpledata(sdb);
    }
    else {
      PartitionedData.Builder pdb = PartitionedData.newBuilder();
      pdb.setPartition(ByteString.copyFrom(partition));

      /*
       * we dont care about byte array
       */
      pdb.setData(ByteString.EMPTY);
      db.setType(Data.DataType.PARTITIONED_DATA);
      db.setPartitioneddata(pdb);
    }

    Tuple t = new Tuple(o);
    t.setContext(context);

    synchronized (this) {
      try {
        while (timemillis == 0) {
          this.wait();
        }
      }
      catch (InterruptedException ie) {
        logger.info("Interrupted while waiting to be in the window because of " + ie.
          getLocalizedMessage());
      }

      db.setWindowId(timemillis); // set it to appropriate window Id
      t.setData(db.build());
      tupleCount++;
      context.sink(t);
    }
  }

  public void beginWindow(long timemillis)
  {
    this.timemillis = timemillis;
    Data.Builder db = Data.newBuilder();
    db.setType(Data.DataType.BEGIN_WINDOW);

    BeginWindow.Builder bwb = BeginWindow.newBuilder();
    bwb.setNode("");
    db.setBeginwindow(bwb);

    Tuple t = new Tuple(null);
    t.setContext(context);

    synchronized (this) {
      db.setWindowId(timemillis);
      t.setData(db.build());
      tupleCount = 0;
      context.sink(t);
      this.notifyAll();
    }
  }

  public void endWindow(long timemillis)
  {
    this.timemillis = 0;
    Data.Builder db = Data.newBuilder();
    db.setType(Data.DataType.END_WINDOW);

    EndWindow.Builder ewb = EndWindow.newBuilder();
    ewb.setNode("");

    Tuple t = new Tuple(null);
    t.setContext(context);

    synchronized (this) {
      ewb.setTupleCount(tupleCount);
      db.setWindowId(timemillis);
      db.setEndwindow(ewb);
      t.setData(db.build());
      context.sink(t);
    }

  }
}

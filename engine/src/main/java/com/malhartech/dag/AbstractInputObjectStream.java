/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.BeginWindow;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.EndWindow;
import com.malhartech.bufferserver.Buffer.PartitionedData;
import com.malhartech.bufferserver.Buffer.SimpleData;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractInputObjectStream implements InputAdapter
{

  protected StreamContext context = null;
  protected long tupleCount = 0;
  
  public void setContext(StreamContext context)
  {
    this.context = context;
  }
  public abstract Object getObject(Object object);

  public Tuple getTuple(Object o)
  {
    SerDe serde = context.getSerDe();
    byte[] partition = serde.getPartition(o);

    Data.Builder db = Data.newBuilder();
    db.setWindowId(0); // set it to appropriate window Id
    if (partition == null) {
      SimpleData.Builder sdb = SimpleData.newBuilder();
      sdb.setData(ByteString.EMPTY);

      /*
       * we dont care about byte array
       */
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
    
    tupleCount++;

    Tuple t = new Tuple(o);
    t.setContext(context);
    t.setData(db.build());
    return t;
  }

  public void beginWindow(long timemillis)
  {
    Data.Builder db = Data.newBuilder();
    db.setWindowId(timemillis);
    db.setType(Data.DataType.BEGIN_WINDOW);

    BeginWindow.Builder bwb = BeginWindow.newBuilder();
    bwb.setNode("");
    db.setBeginwindow(bwb);

    tupleCount = 0;
    
    Tuple t = new Tuple(null);
    t.setContext(context);
    t.setData(db.build());

    context.getSink().doSomething(t);
  }

  public void endWindow(long timemillis)
  {
    Data.Builder db = Data.newBuilder();
    db.setWindowId(timemillis);
    db.setType(Data.DataType.END_WINDOW);
    
    EndWindow.Builder ewb = EndWindow.newBuilder();
    ewb.setNode("");
    ewb.setTupleCount(tupleCount);
    db.setEndwindow(ewb);
    
    Tuple t = new Tuple(null);
    t.setContext(context);
    t.setData(db.build());
    
    context.getSink().doSomething(t);
  }
}

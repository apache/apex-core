/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.BeginWindow;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.Buffer.EndWindow;
import com.malhartech.bufferserver.Buffer.SimpleData;

/**
 * Bunch of utilities shared between tests.
 */
abstract public class StramTestSupport {

  static final class MySerDe implements SerDe
  {
    private Kryo kryo = new Kryo();
    private Output output = new Output(new byte[4096]);
    private Input input = new Input();

    public Object fromByteArray(byte[] bytes)
    {
      input.setBuffer(bytes);
      Object o = kryo.readClassAndObject(input);
      return o;
    }

    public byte[] toByteArray(Object o)
    {
      output.setPosition(0);
      kryo.writeClassAndObject(output, o);
      byte[] bytes = output.toBytes();
      return bytes;
    }

    public byte[] getPartition(Object o)
    {
      return null;
    }
  }
  
  static Tuple generateTuple(Object payload, long windowId, StreamContext sc) {
    Tuple t = new Tuple(payload);
    Data.Builder db = Data.newBuilder();
    db.setType(Data.DataType.SIMPLE_DATA);
    db.setSimpledata(SimpleData.newBuilder().setData(ByteString.EMPTY)).setWindowId(windowId);
    t.setData(db.build());
    t.setContext(sc);
    return t;
  }
  
  static Tuple generateBeginWindowTuple(String nodeid, long windowId, StreamContext sc)
  {
    BeginWindow.Builder bwb = BeginWindow.newBuilder();
    bwb.setNode(nodeid);

    Data.Builder db = Data.newBuilder();
    db.setType(DataType.BEGIN_WINDOW);
    db.setWindowId(windowId);
    db.setBeginwindow(bwb);
    
    Data data = db.build();
    Tuple t = new Tuple(null);
    t.setData(data);
    t.setContext(sc);
    
    return t;
  }
  
  
  static Tuple generateEndWindowTuple(String nodeid, long windowId, int tupleCount, StreamContext sc)
  {
    EndWindow.Builder ewb = EndWindow.newBuilder();
    ewb.setNode(nodeid);
    ewb.setTupleCount(tupleCount);
     
    Data.Builder db = Data.newBuilder();
    db.setType(DataType.END_WINDOW);
    db.setWindowId(windowId);
    db.setEndwindow(ewb);
    
    Data data = db.build();
    Tuple t = new Tuple(null);
    t.setData(data);
    t.setContext(sc);
    
    return t;
  }
  
}

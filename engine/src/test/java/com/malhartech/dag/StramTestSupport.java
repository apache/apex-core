/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.SimpleData;

/**
 * Bunch of utilities shared between tests.
 */
abstract public class StramTestSupport {

  static final class MySerDe implements SerDe
  {

    public Object fromByteArray(byte[] bytes)
    {
      return new String(bytes);
    }

    public byte[] toByteArray(Object o)
    {
      return ((String) o).getBytes();
    }

    public byte[] getPartition(Object o)
    {
      return null;
    }
  }
  
  static Tuple generateTuple(Object payload, StreamContext sc) {
    Tuple t = new Tuple(payload);
    Data.Builder db = Data.newBuilder();
    db.setType(Data.DataType.SIMPLE_DATA);
    db.setSimpledata(SimpleData.newBuilder().setData(ByteString.EMPTY)).setWindowId(0);
    t.setData(db.build());
    t.setContext(sc);
    return t;
  }
  
}

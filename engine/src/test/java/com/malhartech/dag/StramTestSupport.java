/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.SimpleData;

/**
 * Bunch of utilities shared between tests.
 */
abstract public class StramTestSupport {
  private static Logger LOG = LoggerFactory.getLogger(StramTestSupport.class);

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
  
  public static class BufferServerInputSocketStream extends InputSocketStream
  {

    /**
     * Requires upstream node info to setup subscriber TODO: revisit context
     */
    public void setContext(StreamContext context, String upstreamNodeId, String upstreamNodeLogicalName, String downstreamNodeId)
    {
      super.setContext(context);
      String downstreamNodeLogicalName = "downstreamNodeLogicalName"; // TODO: why do we need this?
      ClientHandler.registerPartitions(channel, downstreamNodeId, downstreamNodeLogicalName, upstreamNodeId, upstreamNodeLogicalName, Collections.<String>emptyList());
    }
  }

  public static class BufferServerOutputSocketStream extends OutputSocketStream
  {

    public void setContext(com.malhartech.dag.StreamContext context, String upstreamNodeId, String upstreamNodeLogicalName)
    {
      super.setContext(context);

      // send publisher request
      LOG.info("registering publisher: {} {}", upstreamNodeId, upstreamNodeLogicalName);
      ClientHandler.publish(channel, upstreamNodeId, upstreamNodeLogicalName);
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

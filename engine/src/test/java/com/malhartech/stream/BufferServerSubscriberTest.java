/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.bufferserver.packet.DataTuple;
import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.bufferserver.packet.PayloadTuple;
import com.malhartech.engine.SweepableReservoir;
import com.malhartech.netlet.Client.Fragment;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class BufferServerSubscriberTest
{
  @Test
  public void testEmergencySinks() throws InterruptedException
  {
    final List<Object> list = new ArrayList<Object>();
    final StreamCodec<Object> myserde = new StreamCodec<Object>()
    {
      @Override
      public Object fromByteArray(DataStatePair dspair)
      {
        return dspair.data;
      }

      @Override
      public DataStatePair toByteArray(Object o)
      {
        DataStatePair dsp = new DataStatePair();
        dsp.data = new Fragment((byte[])o, 0, ((byte[])o).length);
        return dsp;
      }

      @Override
      public int getPartition(Object o)
      {
        return 0;
      }

      @Override
      public void resetState()
      {
      }

    };

    Sink<Object> unbufferedSink = new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        list.add(tuple);
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }

    };

    BufferServerSubscriber bss = new BufferServerSubscriber("subscriber", 5)
    {
      {
        serde = myserde;
      }

      @Override
      public void suspendRead()
      {
        logger.debug("read suspended");
      }

      @Override
      public void resumeRead()
      {
        logger.debug("read resumed");
      }

    };

    SweepableReservoir reservoir = bss.acquireReservoir("unbufferedSink", 3);
    reservoir.setSink(unbufferedSink);

    int i = 0;
    while (i++ < 10) {
      DataStatePair dsp = myserde.toByteArray(new byte[]{(byte)i});
      if (dsp.state != null) {
        byte[] buffer = DataTuple.getSerializedTuple(MessageType.CODEC_STATE_VALUE, dsp.state);
        bss.onMessage(buffer, 0, buffer.length);
      }

      byte buffer[] = PayloadTuple.getSerializedTuple(myserde.getPartition(i), dsp.data);
      bss.onMessage(buffer, 0, buffer.length);
    }

    reservoir.sweep(); /* 4 make it to the reservoir */
    reservoir.sweep(); /* we consume the 4; and 4 more make it to the reservoir */
    Assert.assertEquals("4 received", 4, list.size());
    reservoir.sweep(); /* 8 consumed + 2 more make it to the reservoir */
    reservoir.sweep(); /* consume 2 more */
    Assert.assertEquals("10  received", 10, list.size());
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerSubscriberTest.class);
}

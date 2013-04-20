/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
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
      public void process(Object tuple)
      {
        list.add(tuple);
      }

    };

    BufferServerSubscriber bss = new BufferServerSubscriber("subscriber")
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
    while (i++ < 5) {
      bss.onMessage(new byte[] {(byte)i}, 0, 1);
    }

    reservoir.sweep();

    while (i++ < 10) {
      bss.onMessage(new byte[] {(byte)i}, 0, 1);
    }

    reservoir.sweep();
    Assert.assertTrue("tuples received", i - 1 <= list.size());
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerSubscriberTest.class);
}

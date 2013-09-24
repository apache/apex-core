/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.debug;

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.api.Sink;
/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class MuxSinkTest
{
  public MuxSinkTest()
  {
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testAdd()
  {
    Sink<Object> a = new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public int getCount(boolean reset)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    };

    Sink<Object> b = new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public int getCount(boolean reset)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    };

    MuxSink instance1 = new MuxSink(a, b);
    Assert.assertEquals("2 sinks", instance1.getSinks().length, 2);

    MuxSink instance2 = new MuxSink();
    instance2.add(a, b);
    Assert.assertEquals("2 sinks", instance2.getSinks().length, 2);
  }

}

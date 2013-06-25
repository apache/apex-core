/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.debug;

import com.datatorrent.debug.MuxSink;
import com.datatorrent.api.Sink;
import junit.framework.Assert;
import org.junit.Test;

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

/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.api.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Sink;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.stram.tuple.EndStreamTuple;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class GenericNodeTest
{
  public static class GenericOperator implements Operator
  {
    long beginWindowId;
    long endWindowId;
    public final transient DefaultInputPort<Object> ip1 = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        op.emit(tuple);
      }

    };

    @InputPortFieldAnnotation(name = "ip2", optional = true)
    public final transient DefaultInputPort<Object> ip2 = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        op.emit(tuple);
      }

    };

    @OutputPortFieldAnnotation(name="op", optional=true)
    DefaultOutputPort<Object> op = new DefaultOutputPort<Object>();

    @Override
    public void beginWindow(long windowId)
    {
      beginWindowId = windowId;
    }

    @Override
    public void endWindow()
    {
      endWindowId = beginWindowId;
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void teardown()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testSynchingLogic() throws InterruptedException
  {
    long sleeptime = 25L;
    final ArrayList<Object> list = new ArrayList<Object>();
    GenericOperator go = new GenericOperator();
    final GenericNode gn = new GenericNode(go);
    gn.setId(1);
    DefaultReservoir reservoir1 = new DefaultReservoir("ip1Res", 1024);
    DefaultReservoir reservoir2 = new DefaultReservoir("ip2Res", 1024);
    Sink<Object> output = new Sink<Object>()
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

    gn.connectInputPort("ip1", reservoir1);
    gn.connectInputPort("ip2", reservoir2);
    gn.connectOutputPort("op", output);

    final AtomicBoolean ab = new AtomicBoolean(false);
    Thread t = new Thread()
    {
      @Override
      public void run()
      {
        ab.set(true);
        gn.activate(new OperatorContext(0,
                                        this,
                                        new DefaultAttributeMap(),
                                        null));
        gn.run();
        gn.deactivate();
      }

    };
    t.start();

    do {
      Thread.sleep(sleeptime);
    }
    while (ab.get() == false);


    Tuple beginWindow1 = new Tuple(MessageType.BEGIN_WINDOW);
    beginWindow1.setWindowId(0x1L);

    reservoir1.add(beginWindow1);
    Thread.sleep(sleeptime);
    Assert.assertEquals(1, list.size());

    reservoir2.add(beginWindow1);
    Thread.sleep(sleeptime);
    Assert.assertEquals(1, list.size());

    Tuple endWindow1 = new EndWindowTuple(0x1L);

    reservoir1.add(endWindow1);
    Thread.sleep(sleeptime);
    Assert.assertEquals(1, list.size());

    Tuple beginWindow2 = new Tuple(MessageType.BEGIN_WINDOW);
    beginWindow2.setWindowId(0x2L);

    reservoir1.add(beginWindow2);
    Thread.sleep(sleeptime);
    Assert.assertEquals(1, list.size());

    reservoir2.add(endWindow1);
    Thread.sleep(sleeptime);
    Assert.assertEquals(3, list.size());

    reservoir2.add(beginWindow2);
    Thread.sleep(sleeptime);
    Assert.assertEquals(3, list.size());

    Tuple endWindow2 = new EndWindowTuple(0x2L);

    reservoir2.add(endWindow2);
    Thread.sleep(sleeptime);
    Assert.assertEquals(3, list.size());

    reservoir1.add(endWindow2);
    Thread.sleep(sleeptime);
    Assert.assertEquals(4, list.size());

    EndStreamTuple est = new EndStreamTuple(0L);

    reservoir1.add(est);
    Thread.sleep(sleeptime);
    Assert.assertEquals(4, list.size());

    Tuple beginWindow3 = new Tuple(MessageType.BEGIN_WINDOW);
    beginWindow3.setWindowId(0x3L);

    reservoir2.add(beginWindow3);
    Thread.sleep(sleeptime);
    Assert.assertEquals(5, list.size());

    Tuple endWindow3 = new EndWindowTuple(0x3L);

    reservoir2.add(endWindow3);
    Thread.sleep(sleeptime);
    Assert.assertEquals(6, list.size());

    Assert.assertNotSame(Thread.State.TERMINATED, t.getState());

    reservoir2.add(est);
    Thread.sleep(sleeptime);
    Assert.assertEquals(7, list.size());

    Thread.sleep(sleeptime);

    Assert.assertEquals(Thread.State.TERMINATED, t.getState());
  }

}

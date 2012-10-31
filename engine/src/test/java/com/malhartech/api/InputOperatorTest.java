/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.util.CircularBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class InputOperatorTest
{
  static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();

  public static class EvenOddIntegerGeneratorInputOperator implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<Integer> even = new DefaultOutputPort<Integer>(this);
    public final transient DefaultOutputPort<Integer> odd = new DefaultOutputPort<Integer>(this);
    private final transient CircularBuffer<Integer> evenBuffer = new CircularBuffer<Integer>(1024);
    private final transient CircularBuffer<Integer> oddBuffer = new CircularBuffer<Integer>(1024);
    private volatile Thread dataGeneratorThread;

    @Override
    public void replayTuples(long windowId)
    {
    }

    @Override
    public void beginWindow()
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorConfiguration config)
    {
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void postActivate(OperatorContext ctx)
    {
      dataGeneratorThread = new Thread("Integer Emitter")
      {
        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run()
        {
          try {
            int i = 0;
            while (dataGeneratorThread != null) {
              (i % 2 == 0 ? evenBuffer : oddBuffer).put(i++);
              Thread.sleep(20);
            }
          }
          catch (InterruptedException ie) {
          }
        }
      };
      dataGeneratorThread.start();
    }

    @Override
    public void preDeactivate()
    {
      dataGeneratorThread = null;
    }

    @Override
    public void emitTuples(long windowId)
    {
      for (int i = evenBuffer.size(); i-- > 0;) {
        even.emit(evenBuffer.pollUnsafe());
      }
      for (int i = oddBuffer.size(); i-- > 0;) {
        odd.emit(oddBuffer.pollUnsafe());
      }
    }
  }

  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> even = new CollectorInputPort<T>("even", this);
    public final transient CollectorInputPort<T> odd = new CollectorInputPort<T>("odd", this);
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    DAG dag = new DAG();
    EvenOddIntegerGeneratorInputOperator generator = dag.addOperator("NumberGenerator", EvenOddIntegerGeneratorInputOperator.class);
    CollectorModule<Number> collector = dag.addOperator("NumberCollector", new CollectorModule<Number>());

    dag.addStream("EvenIntegers", generator.even, collector.even).setInline(true);
    dag.addStream("OddIntegers", generator.odd, collector.odd).setInline(true);

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException ex) {
        }

        lc.shutdown();
      }
    }.start();

    lc.run();

    Assert.assertEquals("Collections size", 2, collections.size());
    Assert.assertFalse("Zero tuple count", collections.get(collector.even.id).isEmpty() && collections.get(collector.odd.id).isEmpty());
    Assert.assertTrue("Tuple count", collections.get(collector.even.id).size() - collections.get(collector.odd.id).size() <= 1);
  }

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    ArrayList<T> list;
    final String id;

    public CollectorInputPort(String id, Operator module)
    {
      super(module);
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
      list.add(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collections.put(id, list = new ArrayList<T>());
      }
    }
  }
}

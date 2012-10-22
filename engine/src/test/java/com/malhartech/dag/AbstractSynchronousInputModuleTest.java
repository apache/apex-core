/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.*;
import com.malhartech.stram.StramLocalCluster;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class AbstractSynchronousInputModuleTest
{
  static HashMap<String, List> collections = new HashMap<String, List>();

  public static class SynchronousInputOperator implements SyncInputOperator, Runnable
  {
    public final transient DefaultOutputPort<Integer> even = new DefaultOutputPort<Integer>(this);
    public final transient DefaultOutputPort<Integer> odd = new DefaultOutputPort<Integer>(this);

    @Override
    public Runnable getDataPoller()
    {
      return this;
    }

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void run()
    {
      for (int i = 0; i < Integer.MAX_VALUE; i++) {
        (i % 2 == 0 ? even : odd).emit(i);
        try {
          Thread.sleep(20);
        }
        catch (InterruptedException ie) {
          break;
        }
      }
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
    public void activated(OperatorContext context)
    {
    }

    @Override
    public void deactivated()
    {
    }

    @Override
    public void teardown()
    {
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
    SynchronousInputOperator generator = dag.addOperator("NumberGenerator", SynchronousInputOperator.class);
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

    Assert.assertEquals("collections size", 2, collections.size());
    Assert.assertFalse("non zero tuple count", collections.get(collector.even.id).isEmpty() && collections.get(collector.odd.id).isEmpty());
    Assert.assertTrue("tuple count", collections.get(collector.even.id).size() - collections.get(collector.odd.id).size() <= 1);
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

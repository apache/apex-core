/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Operator.Port;
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
  public class SynchronousInputOperator implements SyncInputOperator, Runnable
  {
    public final DefaultOutputPort<Integer> even = new DefaultOutputPort<Integer>(this);
    public final DefaultOutputPort<Integer> odd = new DefaultOutputPort<Integer>(this);

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
    public void setup(OperatorConfiguration config) throws FailedOperationException
    {
    }

    @Override
    public void activate(OperatorContext context)
    {
    }

    @Override
    public void deactivate()
    {
    }

    @Override
    public void teardown()
    {
    }
  }
  static HashMap<Port, List> collections = new HashMap<Port, List>();

  public class CollectorModule implements Operator
  {
    public final CollectorInputPort even = new CollectorInputPort(this);
    public final CollectorInputPort odd = new CollectorInputPort(this);

    @Override
    public void beginWindow()
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorConfiguration config) throws FailedOperationException
    {
    }

    @Override
    public void activate(OperatorContext context)
    {
    }

    @Override
    public void deactivate()
    {
    }

    @Override
    public void teardown()
    {
    }
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    DAG dag = new DAG();
    SynchronousInputOperator generator = dag.addOperator("NumberGenerator", SynchronousInputOperator.class);
    CollectorModule collector = dag.addOperator("NumberCollector", CollectorModule.class);

    dag.addStream("EvenIntegers")
            .setSource(generator.even)
            .addSink(collector.even).setInline(true);

    dag.addStream("OddIntegers")
            .setSource(generator.odd)
            .addSink(collector.odd).setInline(true);

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
    Assert.assertFalse("non zero tuple count", collections.get(collector.even).isEmpty() && collections.get(collector.odd).isEmpty());
    Assert.assertTrue("tuple count", collections.get(collector.even).size() - collections.get(collector.odd).size() <= 1);
  }

  public class CollectorInputPort extends DefaultInputPort<Integer>
  {
    ArrayList<Integer> list;

    public CollectorInputPort(Operator module)
    {
      super(module);
    }

    @Override
    public void process(Integer tuple)
    {
      list.add(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collections.put(this, list = new ArrayList<Integer>());
      }
    }
  }
}

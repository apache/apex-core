/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.api.Operator;
import com.malhartech.api.Sink;
import com.malhartech.dag.DAG.OperatorInstance;
import com.malhartech.stram.StramLocalCluster;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class AbstractSynchronousInputModuleTest
{
  @ModuleAnnotation(ports = {
    @PortAnnotation(name = SynchronousInputOperator.OUTPUT1, type = PortType.OUTPUT),
    @PortAnnotation(name = SynchronousInputOperator.OUTPUT2, type = PortType.OUTPUT)
  })
  public static class SynchronousInputOperator extends AbstractSynchronousInputOperator
  {
    public static final String OUTPUT1 = "OUTPUT1";
    public static final String OUTPUT2 = "OUTPUT2";

    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void run()
    {
      for (int i = 0; i < Integer.MAX_VALUE; i++) {
        emit(i % 2 == 0 ? OUTPUT1 : OUTPUT2, new Integer(i));
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
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void endWindow()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setup(OperatorConfiguration config) throws FailedOperationException
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Sink connect(String port, Sink sink)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void activate(OperatorContext context)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deactivate()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void teardown()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  static HashMap<String, List> collections = new HashMap<String, List>();

  @ModuleAnnotation(ports = {
    @PortAnnotation(name = CollectorModule.INPUT1, type = PortType.INPUT),
    @PortAnnotation(name = CollectorModule.INPUT2, type = PortType.INPUT)
  })
  public static class CollectorModule extends Module implements Sink
  {
    private static final Logger logger = LoggerFactory.getLogger(CollectorModule.class);
    public static final String INPUT1 = "INPUT1";
    public static final String INPUT2 = "INPUT2";

    @Override
    public void connected(String id, Sink dagpart)
    {
      if (dagpart != null) {
        if (!collections.containsKey(id)) {
          collections.put(id, new ArrayList());
        }
      }
    }

    @Override
    public void process(Object payload)
    {
      List l = collections.get(getActivePort());
      l.add(payload);
    }

    public List getTuples(String id)
    {
      return collections.get(id);
    }

    @Override
    public void handleIdleTimeout()
    {
      logger.debug("idling!!!");
    }
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    DAG dag = new DAG();
    OperatorInstance generator = dag.addOperator("NumberGenerator", SynchronousInputOperator.class);
    OperatorInstance collector = dag.addOperator("NumberCollector", CollectorModule.class);

    dag.addStream("EvenIntegers")
            .setSource(generator.getOutput(SynchronousInputOperator.OUTPUT1))
            .addSink(collector.getInput(CollectorModule.INPUT1)).setInline(true);

    dag.addStream("OddIntegers")
            .setSource(generator.getOutput(SynchronousInputOperator.OUTPUT2))
            .addSink(collector.getInput(CollectorModule.INPUT2)).setInline(true);

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
    Assert.assertFalse("non zero tuple count", collections.get(CollectorModule.INPUT1).isEmpty() && collections.get(CollectorModule.INPUT2).isEmpty());
    Assert.assertTrue("tuple count", collections.get(CollectorModule.INPUT1).size() - collections.get(CollectorModule.INPUT2).size() <= 1);
  }
}

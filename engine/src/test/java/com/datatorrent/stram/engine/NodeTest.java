/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.datatorrent.api.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class NodeTest
{
  static class TestGenericOperator implements Operator
  {
    static int beginWindows;
    static int endWindows;

    @Override
    public void beginWindow(long windowId)
    {
      beginWindows++;
    }

    @Override
    public void endWindow()
    {
      endWindows++;
    }

    @Override
    public void setup(OperatorContext context)
    {
      beginWindows = 0;
      endWindows = 0;
    }

    @Override
    public void teardown()
    {
    }

  }

  static class TestInputOperator implements InputOperator
  {
    static int beginWindows;
    static int endWindows;

    @Override
    public void emitTuples()
    {
    }

    @Override
    public void beginWindow(long windowId)
    {
      beginWindows++;
    }

    @Override
    public void endWindow()
    {
      endWindows++;
    }

    @Override
    public void setup(OperatorContext context)
    {
      beginWindows = 0;
      endWindows = 0;
    }

    @Override
    public void teardown()
    {
    }

  }

  public NodeTest()
  {
  }

  @Ignore
  @Test
  public void testStreamingWindowGenericNode() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 10);
    dag.addOperator("GenericOperator", new TestGenericOperator());

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(2000);
  }

  @Stateless
  public static class StatelessOperator implements Operator
  {
    @Override
    public void beginWindow(long windowId)
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void endWindow()
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setup(OperatorContext context)
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void teardown()
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

  }

  public static class StorageAgentImpl implements StorageAgent
  {
    static class Call
    {
      private final String calltype;
      private final int operatorId;
      private final long windowId;

      Call(String calltype, int operatorId, long windowId)
      {
        this.calltype = calltype;
        this.operatorId = operatorId;
        this.windowId = windowId;
      }

    }

    static final ArrayList<Call> calls = new ArrayList<Call>();

    @Override
    public void save(Object object, int operatorId, long windowId) throws IOException
    {
      calls.add(new Call("getSaveStream", operatorId, windowId));
    }

    @Override
    public Object load(int operatorId, long windowId) throws IOException
    {
      calls.add(new Call("getLoadStream", operatorId, windowId));
      return null;
    }

    @Override
    public void delete(int operatorId, long windowId) throws IOException
    {
      calls.add(new Call("delete", operatorId, windowId));
    }


    @Override
    public long[] getWindowIds(int operatorId) throws IOException
    {
      calls.add(new Call("getWindowsIds", operatorId, 0));
      return new long[0];
    }

  }

  @Test
  public void testStatelessOperatorCheckpointing()
  {
    Node<StatelessOperator> node = new Node<StatelessOperator>(new StatelessOperator())
    {
      @Override
      public void connectInputPort(String port, SweepableReservoir reservoir)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public void run()
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

    };

    DefaultAttributeMap attributeMap = new DefaultAttributeMap();
    attributeMap.put(OperatorContext.STORAGE_AGENT, new StorageAgentImpl());
    node.context = new com.datatorrent.stram.engine.OperatorContext(0, Thread.currentThread(), attributeMap, null);
    node.stateless = true;
    synchronized (StorageAgentImpl.calls) {
      StorageAgentImpl.calls.clear();
      node.checkpoint(0);
      Assert.assertEquals("Calls to StorageAgent", 0, StorageAgentImpl.calls.size());
    }
  }

  @Test
  public void testOperatorCheckpointing()
  {
    Node<TestGenericOperator> node = new Node<TestGenericOperator>(new TestGenericOperator())
    {
      @Override
      public void connectInputPort(String port, SweepableReservoir reservoir)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public void run()
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

    };

    DefaultAttributeMap attributeMap = new DefaultAttributeMap();
    attributeMap.put(OperatorContext.STORAGE_AGENT, new StorageAgentImpl());
    node.context = new com.datatorrent.stram.engine.OperatorContext(0, Thread.currentThread(), attributeMap, null);
    synchronized (StorageAgentImpl.calls) {
      StorageAgentImpl.calls.clear();
      node.checkpoint(0);
      Assert.assertEquals("Calls to StorageAgent", 1, StorageAgentImpl.calls.size());
    }
  }

}

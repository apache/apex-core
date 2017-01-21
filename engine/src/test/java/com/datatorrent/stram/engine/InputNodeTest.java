/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.engine;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.IdleTimeHandler;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Sink;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.engine.GenericNodeTest.FSTestWatcher;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.ResetWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

public class InputNodeTest
{
  @Rule
  public FSTestWatcher testMeta = new FSTestWatcher();

  @Test
  public void testEmitTuplesOutsideStreamingWindow() throws Exception
  {
    emitTestHelper(true);
  }

  @Test
  public void testHandleIdleTimeOutsideStreamingWindow() throws Exception
  {
    emitTestHelper(false);
  }

  private void emitTestHelper(boolean trueEmitTuplesFalseHandleIdleTime) throws Exception
  {
    TestInputOperator tio = new TestInputOperator();
    tio.trueEmitTuplesFalseHandleIdleTime = trueEmitTuplesFalseHandleIdleTime;
    DefaultAttributeMap dam = new DefaultAttributeMap();
    dam.put(OperatorContext.APPLICATION_WINDOW_COUNT, 10);
    dam.put(OperatorContext.CHECKPOINT_WINDOW_COUNT, 10);

    final InputNode in = new InputNode(tio, new OperatorContext(0, "operator", dam, null));

    TestSink testSink = new TestSink();

    in.connectInputPort(Node.INPUT, new TestWindowGenerator());
    in.connectOutputPort("output", testSink);

    Thread t = new Thread()
    {
      @Override
      public void run()
      {
        in.activate();
        in.run();
        in.deactivate();
      }
    };
    t.start();

    Thread.sleep(3000);

    in.shutdown();
    t.join();

    Assert.assertTrue("Should have emitted some tuples", testSink.collectedTuples.size() > 0);

    boolean insideWindow = false;

    for (Object tuple : testSink.collectedTuples) {
      if (tuple instanceof Tuple) {
        Tuple controlTuple = (Tuple)tuple;
        MessageType tupleType = controlTuple.getType();

        if (tupleType == MessageType.RESET_WINDOW) {
          Assert.assertFalse(insideWindow);
        } else if (tupleType == MessageType.BEGIN_WINDOW) {
          Assert.assertFalse(insideWindow);
          insideWindow = true;
        } else if (tupleType == MessageType.END_WINDOW) {
          Assert.assertTrue(insideWindow);
          insideWindow = false;
        }
      } else {
        Assert.assertTrue(insideWindow);
      }
    }
  }

  @Test
  public void testDoubleCheckpointAtleastOnce() throws Exception
  {
    NodeTest.testDoubleCheckpointHandling(ProcessingMode.AT_LEAST_ONCE, false, testMeta.getDir());
  }

  @Test
  public void testDoubleCheckpointAtMostOnce() throws Exception
  {
    NodeTest.testDoubleCheckpointHandling(ProcessingMode.AT_MOST_ONCE, false, testMeta.getDir());
  }

  @Test
  public void testDoubleCheckpointExactlyOnce() throws Exception
  {
    NodeTest.testDoubleCheckpointHandling(ProcessingMode.EXACTLY_ONCE, false, testMeta.getDir());
  }

  public static class TestWindowGenerator implements SweepableReservoir
  {
    private final long baseSeconds = (System.currentTimeMillis() / 1000L) << 32;
    private long windowId = 0L;

    private Tuple currentTuple;
    private Sink<Object> oldSink = null;
    private State currentState = State.RESET_WINDOW_NO_TUPLE;
    private long lastTime;

    public enum State
    {
      RESET_WINDOW_NO_TUPLE,
      RESET_WINDOW_TUPLE,
      BEGIN_WINDOW,
      END_WINDOW;
    }

    public TestWindowGenerator()
    {
    }

    @Override
    public Sink<Object> setSink(Sink<Object> sink)
    {
      Sink<Object> tempOldSink = oldSink;
      oldSink = sink;
      return tempOldSink;
    }

    @Override
    public Tuple sweep()
    {
      switch (currentState) {
        case RESET_WINDOW_NO_TUPLE: {
          currentTuple = new ResetWindowTuple(baseSeconds | 500L);
          currentState = State.RESET_WINDOW_TUPLE;
          break;
        }
        case RESET_WINDOW_TUPLE: {
          if (currentTuple == null) {
            currentState = State.BEGIN_WINDOW;
          }
          break;
        }
        case BEGIN_WINDOW: {
          if (System.currentTimeMillis() - lastTime > 1000L) {
            lastTime = System.currentTimeMillis();
            windowId++;
            currentTuple = new Tuple(MessageType.BEGIN_WINDOW, baseSeconds | windowId);
            currentState = State.END_WINDOW;
          }
          break;
        }
        case END_WINDOW: {
          currentTuple = new EndWindowTuple(baseSeconds | windowId);
          currentState = State.BEGIN_WINDOW;
          break;
        }
        default:
          break;
      }

      return currentTuple;
    }

    @Override
    public int getCount(boolean reset)
    {
      return 0;
    }

    @Override
    public int size(final boolean dataTupleAware)
    {
      if (currentTuple != null) {
        return 1;
      } else {
        return 0;
      }
    }

    @Override
    public boolean isEmpty()
    {
      return currentTuple == null;
    }

    @Override
    public Object remove()
    {
      Tuple tempTuple = currentTuple;
      currentTuple = null;
      return tempTuple;
    }

    private static final Logger LOG = LoggerFactory.getLogger(TestWindowGenerator.class);
  }

  private static class TestInputOperator extends BaseOperator implements InputOperator, IdleTimeHandler
  {
    public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<>();

    public boolean trueEmitTuplesFalseHandleIdleTime = true;
    private long lastTimestamp;

    @Override
    public void emitTuples()
    {
      if (trueEmitTuplesFalseHandleIdleTime) {
        emit(100L);
      }
    }

    @Override
    public void handleIdleTime()
    {
      if (!trueEmitTuplesFalseHandleIdleTime) {
        emit(100L);
      }
    }

    private void emit(long delay)
    {
      if (System.currentTimeMillis() - lastTimestamp > delay) {
        lastTimestamp = System.currentTimeMillis();
        output.emit(1L);
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(InputNodeTest.class);
}

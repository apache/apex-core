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

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Sink;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

import static java.lang.Thread.sleep;

/**
 *
 */
public class AtMostOnceTest extends ProcessingModeTests
{
  public AtMostOnceTest()
  {
    super(ProcessingMode.AT_MOST_ONCE);
  }

  @Test
  @Override
  public void testLinearInputOperatorRecovery() throws Exception
  {
    super.testLinearInputOperatorRecovery();
    Assert.assertTrue("Generated Outputs", maxTuples <= CollectorOperator.collection.size());
    Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }

  @Test
  @Override
  public void testLinearOperatorRecovery() throws Exception
  {
    super.testLinearOperatorRecovery();
    Assert.assertTrue("Generated Outputs", maxTuples >= CollectorOperator.collection.size());
    Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }

  @Test
  @Override
  public void testLinearInlineOperatorsRecovery() throws Exception
  {
    super.testLinearInlineOperatorsRecovery();
    Assert.assertTrue("Generated Outputs", maxTuples >= CollectorOperator.collection.size());
    Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Override
  public void testNonLinearOperatorRecovery() throws InterruptedException
  {
    final HashSet<Object> collection = new HashSet<>();

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap map = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    map.put(OperatorContext.CHECKPOINT_WINDOW_COUNT, 0);
    map.put(OperatorContext.PROCESSING_MODE, processingMode);

    final GenericNode node = new GenericNode(new MultiInputOperator(),
        new com.datatorrent.stram.engine.OperatorContext(1, "operator", map, null));
    AbstractReservoir reservoir1 = AbstractReservoir.newReservoir("input1", 1024);
    AbstractReservoir reservoir2 = AbstractReservoir.newReservoir("input1", 1024);
    node.connectInputPort("input1", reservoir1);
    node.connectInputPort("input2", reservoir2);
    node.connectOutputPort("output", new Sink<Object>()
    {
      @Override
      public void put(Object t)
      {
        if (collection.contains(t)) {
          throw new RuntimeException("Duplicate Found!");
        }

        collection.add(t);
      }

      @Override
      public int getCount(boolean bln)
      {
        return 0;
      }

    });

    final AtomicBoolean active = new AtomicBoolean(false);
    Thread thread = new Thread()
    {
      @Override
      public void run()
      {
        active.set(true);
        node.activate();
        node.run();
        node.deactivate();
      }

    };
    thread.start();

    for (int i = 0; i < 100 && !active.get(); i++) {
      sleep(5);
    }

    reservoir1.add(new Tuple(MessageType.BEGIN_WINDOW, 1));
    reservoir1.add(1);
    reservoir2.add(new Tuple(MessageType.BEGIN_WINDOW, 1));
    reservoir1.add(new EndWindowTuple(1));
    reservoir2.add(1);
    reservoir2.add(new EndWindowTuple(1));

    for (int i = 0; i < 100 && collection.size() < 4; i++) {
      sleep(5);
    }

    reservoir1.add(new Tuple(MessageType.BEGIN_WINDOW, 2));
    reservoir1.add(2);
    reservoir1.add(new EndWindowTuple(2));

    for (int i = 0; i < 100 && collection.size() < 6; i++) {
      sleep(5);
    }

    reservoir2.add(new Tuple(MessageType.BEGIN_WINDOW, 4));
    reservoir2.add(4);
    reservoir2.add(new EndWindowTuple((4)));

    for (int i = 0; i < 100 && collection.size() < 9; i++) {
      sleep(5);
    }

    reservoir1.add(new Tuple(MessageType.BEGIN_WINDOW, 3));
    reservoir1.add(3);
    reservoir1.add(new EndWindowTuple(3));

    sleep(500);

    reservoir1.add(new Tuple(MessageType.BEGIN_WINDOW, 5));
    reservoir1.add(5);
    reservoir2.add(new Tuple(MessageType.BEGIN_WINDOW, 5));
    reservoir1.add(new EndWindowTuple(5));
    reservoir2.add(5);
    reservoir2.add(new EndWindowTuple(5));

    for (int i = 0; i < 100 && collection.size() < 14; i++) {
      sleep(5);
    }

    thread.interrupt();
    thread.join();

    /* lets make sure that we have all the tuples and nothing more */
    for (Object o : collection) {
      if (o instanceof Tuple) {
        Tuple t = (Tuple)o;
        long windowId = t.getWindowId();
        Assert.assertTrue("Valid Window Id", windowId == 1 || windowId == 2 || windowId == 4 || windowId == 5);
        Assert.assertTrue("Valid Tuple Type", t.getType() == MessageType.BEGIN_WINDOW || t.getType() == MessageType.END_WINDOW || t.getType() == MessageType.END_STREAM);
      } else {
        switch (((Integer)o).intValue()) {
          case 101:
          case 201:
          case 102:
          case 204:
          case 105:
          case 205:
            break;

          default:
            Assert.fail("Unexpected Data Tuple: " + o);
        }
      }
    }
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(AtMostOnceTest.class);
}

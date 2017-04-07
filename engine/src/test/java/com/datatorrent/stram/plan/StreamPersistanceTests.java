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
package com.datatorrent.stram.plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAG.OperatorMeta;
import com.datatorrent.api.DAG.StreamMeta;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.PartitioningTest;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.StreamingContainerManagerTest;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.DefaultKryoStreamCodec;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.StreamCodecWrapperForPersistance;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.StramTestSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamPersistanceTests
{
  static final Logger logger = LoggerFactory.getLogger(StreamPersistanceTests.class);

  public static class TestReceiverOperator extends BaseOperator
  {
    public static volatile List<Object> results = new ArrayList<>();
    public volatile AtomicInteger size = new AtomicInteger(0);

    @InputPortFieldAnnotation(optional = true)
    public final transient InputPort<Object> inport = new DefaultInputPort<Object>()
    {
      @Override
      public final void process(Object t)
      {
        results.add(t);
        size.incrementAndGet();
      }
    };
  }

  public static class TestPersistanceOperator implements Operator
  {
    public static volatile List<Object> results = new ArrayList<>();

    @InputPortFieldAnnotation(optional = true)
    public final transient InputPort<Object> inport = new DefaultInputPort<Object>()
    {
      @Override
      public final void process(Object t)
      {
        results.add(t);
      }
    };

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

  }

  public static class PartitionedTestPersistanceOperator extends TestPersistanceOperator implements Partitioner<PartitionedTestPersistanceOperator>
  {
    @Override
    public Collection definePartitions(Collection partitions, PartitioningContext context)
    {
      Collection<Partition> newPartitions = new ArrayList<>();

      int partitionMask = 0x03;

      // No partitioning done so far..
      // Single partition with mask 0x03 and set {0}
      // First partition
      PartitionedTestPersistanceOperator newInstance = new PartitionedTestPersistanceOperator();
      Partition partition = new DefaultPartition<>(newInstance);
      PartitionKeys value = new PartitionKeys(partitionMask, Sets.newHashSet(0));
      partition.getPartitionKeys().put(inport, value);
      newPartitions.add(partition);

      return newPartitions;
    }

    @Override
    public void partitioned(Map partitions)
    {
      // TODO Auto-generated method stub
    }
  }

  public class TestOperatorWithOutputPorts extends BaseOperator
  {

    @InputPortFieldAnnotation(optional = true)
    public final transient DefaultInputPort<Object> inputPort = new DefaultInputPort<Object>()
    {
      @Override
      public final void process(Object t)
      {
        // Do nothing: Dummy operator for test
      }
    };

    @InputPortFieldAnnotation(optional = false)
    public final transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<>();
  }

  public class TestOperatorWithMultipleNonOptionalInputPorts extends BaseOperator
  {

    @InputPortFieldAnnotation(optional = false)
    public final transient DefaultInputPort<Object> inputPort1 = new DefaultInputPort<Object>()
    {
      @Override
      public final void process(Object t)
      {
        // Do nothing: Dummy operator for test
      }
    };

    @InputPortFieldAnnotation(optional = false)
    public final transient DefaultInputPort<Object> inputPort2 = new DefaultInputPort<Object>()
    {
      @Override
      public final void process(Object t)
      {
        // Do nothing: Dummy operator for test
      }
    };

    public final transient DefaultInputPort<Object> inputPort3 = new DefaultInputPort<Object>()
    {
      @Override
      public final void process(Object t)
      {
        // Do nothing: Dummy operator for test
      }
    };
  }

  public class TestOperatorWithoutInputPorts extends BaseOperator
  {
  }

  private LogicalPlan dag;

  @Before
  public void setup()
  {
    dag = StramTestSupport.createDAG(testMeta);
  }

  @Test
  public void testPersistStreamOperatorIsAdded()
  {
    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator x = dag.addOperator("x", new GenericTestOperator());
    TestReceiverOperator persister = new TestReceiverOperator();
    StreamMeta stream = dag.addStream("Stream1", input1.outport, x.inport1);
    stream.persistUsing("Stream1_persister",persister, persister.inport);

    // Check operator is added to dag
    OperatorMeta persistOperatorMeta = dag.getOperatorMeta("Stream1_persister");
    assertEquals("Persist operator not added to dag ", persister, persistOperatorMeta.getOperator());
    dag.validate();
  }

  @Test
  public void testPersistStreamOperatorIsAddedPerSink()
  {
    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator x1 = dag.addOperator("x1", new GenericTestOperator());
    GenericTestOperator x2 = dag.addOperator("x2", new GenericTestOperator());
    GenericTestOperator x3 = dag.addOperator("x3", new GenericTestOperator());

    TestReceiverOperator persister = new TestReceiverOperator();
    TestReceiverOperator persister1 = new TestReceiverOperator();
    TestReceiverOperator persister2 = new TestReceiverOperator();

    StreamMeta stream = dag.addStream("Stream1", input1.outport, x1.inport1, x2.inport1, x3.inport1);

    stream.persistUsing("Stream1_persister", persister, persister.inport);
    stream.persistUsing("Stream1_x1_persister", persister1, persister1.inport, x1.inport1);
    stream.persistUsing("Stream1_x2_persister", persister2, persister2.inport, x2.inport1);

    // Check 3 persist operators are added to dag
    OperatorMeta persistOperatorMeta = dag.getOperatorMeta("Stream1_persister");
    assertEquals("Persist operator not added to dag ", persister, persistOperatorMeta.getOperator());

    persistOperatorMeta = dag.getOperatorMeta("Stream1_x1_persister");
    assertEquals("Persist operator not added to dag ", persister1, persistOperatorMeta.getOperator());

    persistOperatorMeta = dag.getOperatorMeta("Stream1_x2_persister");
    assertEquals("Persist operator not added to dag ", persister2, persistOperatorMeta.getOperator());

    dag.validate();
  }

  @Test
  public void testaddStreamThrowsExceptionOnInvalidLoggerType()
  {
    // Test Logger with non-optional output ports
    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator x = dag.addOperator("x", new GenericTestOperator());
    StreamMeta stream = dag.addStream("Stream1", input1.outport, x.inport1);

    TestOperatorWithOutputPorts persister = new TestOperatorWithOutputPorts();
    try {
      stream.persistUsing("persister", persister, persister.inputPort);
      Assert.fail("should throw Illegal argument exception: Persist operator has non optional output ports");
    } catch (IllegalArgumentException e) {
      logger.debug(e.getMessage());
    }

    // Test already added operator passed
    TestOperatorWithOutputPorts persister1 = new TestOperatorWithOutputPorts();
    try {
      stream.persistUsing("Stream1_persister", persister1, persister1.inputPort);
      Assert.fail("should throw exception that Stream1_persister object was already added");
    } catch (IllegalArgumentException e) {
      logger.debug(e.getMessage());
    }

    // Test persist operator without any input ports
    dag.removeOperator(dag.getOperatorMeta("Stream1_persister").getOperator());
    TestOperatorWithoutInputPorts logger2 = new TestOperatorWithoutInputPorts();
    try {
      stream.persistUsing("Stream1_persister", logger2);
      Assert.fail("should throw Illegal argument exception: persist operator should have input ports");
    } catch (IllegalArgumentException e) {
      logger.debug(e.getMessage());
    }

    // Test persist operator with more than one input port as non-optional
    dag.removeOperator(dag.getOperatorMeta("Stream1_persister").getOperator());
    TestOperatorWithMultipleNonOptionalInputPorts persister3 = new TestOperatorWithMultipleNonOptionalInputPorts();
    try {
      stream.persistUsing("Stream1_persister", persister3);
      Assert.fail("should throw Illegal argument exception: persist operator should have at most 1 non-optional input port");
    } catch (IllegalArgumentException e) {
      logger.debug(e.getMessage());
    }
  }

  @Test
  public void testaddStreamThrowsExceptionOnInvalidInputPortForLoggerType()
  {
    // Test for input port belonging to different object
    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator x = dag.addOperator("x", new GenericTestOperator());
    TestReceiverOperator persister = new TestReceiverOperator();
    TestReceiverOperator persister1 = new TestReceiverOperator();
    StreamMeta stream = dag.addStream("Stream1", input1.outport, x.inport1);
    try {
      stream.persistUsing("Stream1_persister", persister, persister1.inport);
      Assert.fail("should throw Illegal argument exception: Port passed does not belong to operator class");
    } catch (IllegalArgumentException e) {
      // all good
    }

    // Remove persist operator from dag
    dag.removeOperator(dag.getOperatorMeta("Stream1_persister").getOperator());
  }

  @Test
  public void testPersistStreamOperatorIsRemovedWhenStreamIsRemoved()
  {
    // Remove Stream and check if persist operator is removed
    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator x = dag.addOperator("x", new GenericTestOperator());
    TestReceiverOperator persister = new TestReceiverOperator();
    StreamMeta stream = dag.addStream("Stream1", input1.outport, x.inport1);
    stream.persistUsing("Stream1_persister", persister, persister.inport);

    ((LogicalPlan.StreamMeta)stream).remove();

    // Check operator is added to dag
    OperatorMeta persistOperatorMeta = dag.getOperatorMeta("Stream1_persister");
    assertEquals("Persist operator should be removed from dag after stream.remove", null, persistOperatorMeta);
  }

  @Test
  public void testPersistStreamOperatorIsRemovedWhenSinkIsRemoved()
  {
    // Remove sink and check if corresponding persist operator is removed
    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator x1 = dag.addOperator("x1", new GenericTestOperator());
    GenericTestOperator x2 = dag.addOperator("x2", new GenericTestOperator());
    GenericTestOperator x3 = dag.addOperator("x3", new GenericTestOperator());

    TestReceiverOperator persister = new TestReceiverOperator();
    TestReceiverOperator persister1 = new TestReceiverOperator();
    TestReceiverOperator persister2 = new TestReceiverOperator();

    StreamMeta stream = dag.addStream("Stream1", input1.outport, x1.inport1, x2.inport1, x3.inport1);

    stream.persistUsing("Stream1_persister", persister, persister.inport);
    stream.persistUsing("Stream1_x1_persister", persister1, persister1.inport, x1.inport1);
    stream.persistUsing("Stream1_x2_persister", persister2, persister2.inport, x2.inport1);

    // Check 3 persist operators are added to dag
    OperatorMeta persistOperatorMeta = dag.getOperatorMeta("Stream1_persister");
    assertEquals("Persist operator not added to dag ", persister, persistOperatorMeta.getOperator());

    persistOperatorMeta = dag.getOperatorMeta("Stream1_x1_persister");
    assertEquals("Persist operator not added to dag ", persister1, persistOperatorMeta.getOperator());

    persistOperatorMeta = dag.getOperatorMeta("Stream1_x2_persister");
    assertEquals("Persist operator not added to dag ", persister2, persistOperatorMeta.getOperator());

    dag.removeOperator(x1);
    // Check persister for x1 is removed
    persistOperatorMeta = dag.getOperatorMeta("Stream1_x1_persister");
    assertEquals("Persist operator should be removed from dag after sink is removed", null, persistOperatorMeta);

    // Check other persisters are unchanged

    persistOperatorMeta = dag.getOperatorMeta("Stream1_persister");
    assertEquals("Persist operator not added to dag ", persister, persistOperatorMeta.getOperator());

    persistOperatorMeta = dag.getOperatorMeta("Stream1_x2_persister");
    assertEquals("Persist operator not added to dag ", persister2, persistOperatorMeta.getOperator());
  }

  @Test
  public void testPersistStreamOperatorIsRemovedWhenAllSinksAreRemoved()
  {
    TestGeneratorInputOperator input1 = dag.addOperator("input1", TestGeneratorInputOperator.class);
    GenericTestOperator x1 = dag.addOperator("x1", new GenericTestOperator());
    GenericTestOperator x2 = dag.addOperator("x2", new GenericTestOperator());
    GenericTestOperator x3 = dag.addOperator("x3", new GenericTestOperator());

    TestReceiverOperator persister = new TestReceiverOperator();

    StreamMeta stream = dag.addStream("Stream1", input1.outport, x1.inport1, x2.inport1, x3.inport1);

    stream.persistUsing("Stream1_persister", persister, persister.inport);

    // Check stream persister is added to the dag
    Assert.assertNotNull("Stream persister operator should be present", dag.getOperatorMeta("Stream1_persister"));

    // remove sink operators x1, x2, x3 from dag and check that persister
    // operator is removed
    dag.removeOperator(x1);
    dag.removeOperator(x2);
    dag.removeOperator(x3);
    Assert.assertNull("Persister operator should have been removed after all sinks are removed", dag.getOperatorMeta("Stream1_persister"));
  }

  @Test
  public void testPersistStreamOperatorGeneratesIdenticalOutputAsSink() throws ClassNotFoundException, IOException, InterruptedException
  {
    AscendingNumbersOperator input1 = dag.addOperator("input1", AscendingNumbersOperator.class);
    // Add PersistOperator directly to dag
    final TestReceiverOperator x = dag.addOperator("x", new TestReceiverOperator());
    StreamMeta stream = dag.addStream("Stream1", input1.outputPort, x.inport);

    // Use an instance of PersistOperator to persist stream
    TestPersistanceOperator persister = new TestPersistanceOperator();
    stream.persistUsing("Stream1_persister", persister, persister.inport);

    runLocalClusterAndValidate(dag, x, persister);
  }

  private void runLocalClusterAndValidate(LogicalPlan dag, final TestReceiverOperator x, final TestPersistanceOperator persister) throws IOException, ClassNotFoundException
  {
    try {
      x.results.clear();
      persister.results.clear();
      // Run local cluster and verify both results are identical
      final StramLocalCluster lc = new StramLocalCluster(dag);

      new Thread("LocalClusterController")
      {
        @Override
        public void run()
        {
          long startTms = System.currentTimeMillis();
          long timeout = 100000L;
          try {
            while (System.currentTimeMillis() - startTms < timeout) {
              if (x.results.size() < 1000) {
                Thread.sleep(10);
              } else {
                break;
              }
            }
          } catch (Exception ex) {
            throw Throwables.propagate(ex);
          } finally {
            lc.shutdown();
          }
        }

      }.start();

      lc.run();
      int maxTuples = x.results.size() > persister.results.size() ? persister.results.size() : x.results.size();
      // Output of both operators should be identical
      for (int i = 0; i < maxTuples; i++) {
        logger.debug("Tuple = " + x.results.get(i) + " - " + persister.results.get(i));
        assertEquals("Mismatch observed for tuple ", x.results.get(i), persister.results.get(i));
      }
    } finally {
      x.results.clear();
      persister.results.clear();
    }
  }

  public static class AscendingNumbersOperator implements InputOperator
  {

    private Integer count = 0;

    @Override
    public void emitTuples()
    {

      outputPort.emit(count++);
    }

    public final transient DefaultOutputPort<Integer> outputPort = new DefaultOutputPort<>();

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

  }

  public static class DivisibleByStreamCodec extends DefaultKryoStreamCodec
  {

    protected int number = 1;

    public DivisibleByStreamCodec()
    {
      super();
    }

    public DivisibleByStreamCodec(int number)
    {
      super();
      this.number = number;
    }

    @Override
    public int getPartition(Object o)
    {
      if ((Integer)o % number == 0) {
        return 1;
      }
      return 2;
    }

  }

  public static class PassThruOperatorWithCodec extends BaseOperator implements Partitioner<PassThruOperatorWithCodec>
  {

    private int divisibleBy = 1;

    public PassThruOperatorWithCodec()
    {
    }

    public PassThruOperatorWithCodec(int divisibleBy)
    {
      this.divisibleBy = divisibleBy;
    }

    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        output.emit(tuple);
      }

      @Override
      public StreamCodec<Object> getStreamCodec()
      {
        return new DivisibleByStreamCodec(divisibleBy);
      }
    };

    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

    @Override
    public Collection definePartitions(Collection partitions, PartitioningContext context)
    {
      Collection<Partition> newPartitions = new ArrayList<>();

      // Mostly for 1 partition we dont need to do this
      int partitionBits = (Integer.numberOfLeadingZeros(0) - Integer.numberOfLeadingZeros(1));
      int partitionMask = 0;
      if (partitionBits > 0) {
        partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
      }

      partitionMask = 1;

      if (partitions.size() == 1) {
        // No partitioning done so far..
        // Single partition again, but with only even numbers ok?
        PassThruOperatorWithCodec newInstance = new PassThruOperatorWithCodec();
        Partition partition = new DefaultPartition<>(newInstance);

        // Consider partitions are 1 & 2 and we are sending only 1 partition
        // Partition 1 = even numbers
        // Partition 2 = odd numbers
        PartitionKeys value = new PartitionKeys(partitionMask, Sets.newHashSet(1));
        partition.getPartitionKeys().put(input, value);
        newPartitions.add(partition);
      }

      return newPartitions;
    }

    @Override
    public void partitioned(Map partitions)
    {
      // TODO Auto-generated method stub

    }
  }

  @Test
  public void testPersistStreamWithFiltering() throws ClassNotFoundException, IOException, InterruptedException
  {
    AscendingNumbersOperator ascend = dag.addOperator("ascend", new AscendingNumbersOperator());
    PassThruOperatorWithCodec passThru = dag.addOperator("PassThrough", new PassThruOperatorWithCodec(2));
    TestReceiverOperator console = dag.addOperator("console", new TestReceiverOperator());
    TestPersistanceOperator console1 = new TestPersistanceOperator();
    StreamMeta s = dag.addStream("Stream1", ascend.outputPort, passThru.input);
    s.persistUsing("Stream1_persister", console1, console1.inport);
    dag.addStream("Stream2", passThru.output, console.inport);
    runLocalClusterAndValidate(dag, console, console1);
  }

  @Test
  public void testPersistStreamOnSingleSinkWithFiltering() throws ClassNotFoundException, IOException, InterruptedException
  {
    AscendingNumbersOperator ascend = dag.addOperator("ascend", new AscendingNumbersOperator());
    PassThruOperatorWithCodec passThru = dag.addOperator("PassThrough", new PassThruOperatorWithCodec(2));
    final TestReceiverOperator console = dag.addOperator("console", new TestReceiverOperator());

    TestPersistanceOperator persister = new TestPersistanceOperator();
    StreamMeta s = dag.addStream("Stream1", ascend.outputPort, passThru.input);
    s.persistUsing("Stream1_persister", persister, persister.inport, passThru.input);
    dag.addStream("Stream2", passThru.output, console.inport);
    runLocalClusterAndValidate(dag, console, persister);
  }

  @Test
  public void testPersistStreamOnSingleSinkWithFilteringContainerLocal() throws ClassNotFoundException, IOException, InterruptedException
  {
    AscendingNumbersOperator ascend = dag.addOperator("ascend", new AscendingNumbersOperator());
    PassThruOperatorWithCodec passThru = dag.addOperator("PassThrough", new PassThruOperatorWithCodec(2));
    PassThruOperatorWithCodec passThru2 = dag.addOperator("Multiples_of_3", new PassThruOperatorWithCodec(3));

    final TestReceiverOperator console = dag.addOperator("console", new TestReceiverOperator());
    final TestReceiverOperator console1 = dag.addOperator("console1", new TestReceiverOperator());

    TestPersistanceOperator persister = new TestPersistanceOperator();
    StreamMeta s = dag.addStream("Stream1", ascend.outputPort, passThru.input, passThru2.input).setLocality(Locality.CONTAINER_LOCAL);
    s.persistUsing("persister", persister, persister.inport);
    dag.addStream("Stream2", passThru.output, console.inport);
    dag.addStream("Stream3", passThru2.output, console1.inport);
    // runLocalClusterAndValidate(dag, console, persister);

    persister.results.clear();
    console.results.clear();
    console1.results.clear();

    // Validate union of results is received on persist operator
    final StramLocalCluster lc = new StramLocalCluster(dag);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        long startTms = System.currentTimeMillis();
        long timeout = 1000000L;
        try {
          while (System.currentTimeMillis() - startTms < timeout) {
            if ((console.results.size() < 6) || (console.results.size() < 6)) {
              Thread.sleep(10);
            } else {
              break;
            }
          }
        } catch (Exception ex) {
          throw Throwables.propagate(ex);
        } finally {
          lc.shutdown();
        }
      }

    }.start();

    lc.run();
    try {
      Integer[] expectedResult = {0, 2, 3, 4, 6, 8, 9, 10, 12};
      for (int i = 0; i < expectedResult.length; i++) {
        logger.debug(persister.results.get(i) + " " + expectedResult[i]);
        assertEquals("Mismatch observed for tuple ", expectedResult[i], persister.results.get(i));
      }
    } finally {

      persister.results.clear();
      console.results.clear();
      console1.results.clear();
    }
  }

  @Test
  public void testPersistStreamOperatorGeneratesUnionOfAllSinksOutput() throws ClassNotFoundException, IOException
  {
    AscendingNumbersOperator ascend = dag.addOperator("ascend", new AscendingNumbersOperator());
    PassThruOperatorWithCodec passThru1 = dag.addOperator("PassThrough1", new PassThruOperatorWithCodec(2));
    PassThruOperatorWithCodec passThru2 = dag.addOperator("PassThrough2", new PassThruOperatorWithCodec(3));

    final TestReceiverOperator console = dag.addOperator("console", new TestReceiverOperator());
    final TestReceiverOperator console1 = dag.addOperator("console1", new TestReceiverOperator());

    TestPersistanceOperator persister = new TestPersistanceOperator();
    StreamMeta s = dag.addStream("Stream1", ascend.outputPort, passThru1.input, passThru2.input);
    s.persistUsing("persister", persister, persister.inport);

    dag.addStream("Stream2", passThru1.output, console.inport);
    dag.addStream("Stream3", passThru2.output, console1.inport);

    persister.results.clear();
    console.results.clear();
    console1.results.clear();

    // Validate union of results is received on persist operator
    final StramLocalCluster lc = new StramLocalCluster(dag);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        long startTms = System.currentTimeMillis();
        long timeout = 10000L;
        try {
          while (System.currentTimeMillis() - startTms < timeout) {
            if ((console.results.size() < 6) || (console.results.size() < 6)) {
              Thread.sleep(10);
            } else {
              break;
            }
          }
        } catch (Exception ex) {
          throw Throwables.propagate(ex);
        } finally {
          lc.shutdown();
        }
      }

    }.start();

    lc.run();
    try {
      Integer[] expectedResult = {0, 2, 3, 4, 6, 8, 9, 10, 12};
      for (int i = 0; i < expectedResult.length; i++) {
        logger.debug(persister.results.get(i) + " " + expectedResult[i]);
        assertEquals("Mismatch observed for tuple ", expectedResult[i], persister.results.get(i));
      }
    } finally {

      persister.results.clear();
      console.results.clear();
      console1.results.clear();
    }
  }

  public static class TestPartitionCodec extends DefaultKryoStreamCodec
  {

    public TestPartitionCodec()
    {
      super();
    }

    @Override
    public int getPartition(Object o)
    {
      return (int)o;// & 0x03;
    }

  }

  public static class PartitionedTestOperatorWithFiltering extends BaseOperator implements Partitioner<PassThruOperatorWithCodec>
  {

    public PartitionedTestOperatorWithFiltering()
    {
    }

    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        output.emit(tuple);
      }
    };

    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

    @Override
    public Collection definePartitions(Collection partitions, PartitioningContext context)
    {
      Collection<Partition> newPartitions = new ArrayList<>();

      int partitionMask = 0x03;

      // No partitioning done so far..
      // Single partition again, but with only even numbers ok?
      // First partition
      PassThruOperatorWithCodec newInstance = new PassThruOperatorWithCodec();
      Partition partition = new DefaultPartition<>(newInstance);
      PartitionKeys value = new PartitionKeys(partitionMask, Sets.newHashSet(0));
      partition.getPartitionKeys().put(input, value);
      newPartitions.add(partition);

      // Second partition
      newInstance = new PassThruOperatorWithCodec();
      partition = new DefaultPartition<>(newInstance);
      value = new PartitionKeys(partitionMask, Sets.newHashSet(1));
      partition.getPartitionKeys().put(input, value);

      newPartitions.add(partition);

      return newPartitions;
    }

    @Override
    public void partitioned(Map partitions)
    {
      logger.debug("Dynamic partitioning done....");
    }
  }

  @Test
  public void testPersistStreamOperatorMultiplePhysicalOperatorsForSink() throws ClassNotFoundException, IOException
  {
    AscendingNumbersOperator ascend = dag.addOperator("ascend", new AscendingNumbersOperator());
    PartitionedTestOperatorWithFiltering passThru = dag.addOperator("partition", new PartitionedTestOperatorWithFiltering());
    final TestReceiverOperator console = dag.addOperator("console", new TestReceiverOperator());
    final TestPersistanceOperator console1 = new TestPersistanceOperator();
    StreamMeta s = dag.addStream("Stream1", ascend.outputPort, passThru.input);
    dag.setInputPortAttribute(passThru.input, PortContext.STREAM_CODEC, new TestPartitionCodec());
    s.persistUsing("persister", console1, console1.inport);
    dag.addStream("Stream2", passThru.output, console.inport);

    final StramLocalCluster lc = new StramLocalCluster(dag);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        long startTms = System.currentTimeMillis();
        long timeout = 100000L;
        try {
          while (System.currentTimeMillis() - startTms < timeout) {
            if ((console.results.size() < 6) || (console1.results.size() < 6)) {
              Thread.sleep(10);
            } else {
              break;
            }
          }
        } catch (Exception ex) {
          throw Throwables.propagate(ex);
        } finally {
          lc.shutdown();
        }
      }

    }.start();

    lc.run();

    try {
      Integer[] expectedResult = {0, 1, 4, 5, 8, 9, 12, 13, 16};

      for (int i = 0; i < expectedResult.length; i++) {
        logger.debug(console1.results.get(i) + " " + expectedResult[i]);
        assertEquals("Mismatch observed for tuple ", expectedResult[i], console1.results.get(i));
      }
    } finally {
      console1.results.clear();
      console.results.clear();
    }
  }

  @Test
  public void testPartitionedPersistOperator() throws ClassNotFoundException, IOException
  {
    AscendingNumbersOperator ascend = dag.addOperator("ascend", new AscendingNumbersOperator());
    PartitionedTestOperatorWithFiltering passThru = dag.addOperator("partition", new PartitionedTestOperatorWithFiltering());
    final TestReceiverOperator console = dag.addOperator("console", new TestReceiverOperator());
    final PartitionedTestPersistanceOperator console1 = new PartitionedTestPersistanceOperator();
    StreamMeta s = dag.addStream("Stream1", ascend.outputPort, passThru.input);
    dag.setInputPortAttribute(passThru.input, PortContext.STREAM_CODEC, new TestPartitionCodec());
    s.persistUsing("persister", console1, console1.inport);
    dag.setInputPortAttribute(console1.inport, PortContext.STREAM_CODEC, new TestPartitionCodec());
    dag.addStream("Stream2", passThru.output, console.inport);

    final StramLocalCluster lc = new StramLocalCluster(dag);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        long startTms = System.currentTimeMillis();
        long timeout = 100000L;
        try {
          while (System.currentTimeMillis() - startTms < timeout) {
            if (console1.results.size() < 6) {
              Thread.sleep(10);
            } else {
              break;
            }
          }
        } catch (Exception ex) {
          throw Throwables.propagate(ex);
        } finally {
          lc.shutdown();
        }
      }

    }.start();

    lc.run();

    try {
      // Values as per persist operator's partition keys should be picked up
      Integer[] expectedResult = {0, 4, 8, 12, 16, 20 };

      for (int i = 0; i < expectedResult.length; i++) {
        logger.debug(console1.results.get(i) + " " + expectedResult[i]);
        assertEquals("Mismatch observed for tuple ", expectedResult[i], console1.results.get(i));
      }
    } finally {
      console1.results.clear();
      console.results.clear();
    }
  }

  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  @Test
  public void testDynamicPartitioning() throws ClassNotFoundException, IOException
  {
    AscendingNumbersOperator ascend = dag.addOperator("ascend", new AscendingNumbersOperator());

    final TestReceiverOperator console = dag.addOperator("console", new TestReceiverOperator());
    dag.setOperatorAttribute(console, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<TestReceiverOperator>(2));
    dag.setOperatorAttribute(console, Context.OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)new PartitioningTest.PartitionLoadWatch()));

    final PartitionedTestPersistanceOperator console1 = new PartitionedTestPersistanceOperator();

    StreamMeta s = dag.addStream("Stream1", ascend.outputPort, console.inport);
    dag.setInputPortAttribute(console.inport, PortContext.STREAM_CODEC, new TestPartitionCodec());
    s.persistUsing("persister", console1, console1.inport);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 4, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta passThruMeta = dag.getMeta(console);

    List<PTOperator> ptos = plan.getOperators(passThruMeta);

    PTOperator persistOperatorContainer = null;

    for (PTContainer container : plan.getContainers()) {
      for (PTOperator operator : container.getOperators()) {
        operator.setState(PTOperator.State.ACTIVE);
        if (operator.getName().equals("persister")) {
          persistOperatorContainer = operator;
        }
      }
    }

    // Check that persist operator is part of dependents redeployed
    Set<PTOperator> operators = plan.getDependents(ptos);
    logger.debug("Operators to be re-deployed = {}", operators);
    // Validate that persist operator is part of dependents
    assertTrue("persist operator should be part of the operators to be redeployed", operators.contains(persistOperatorContainer));

    LogicalPlan.StreamMeta s1 = (LogicalPlan.StreamMeta)s;
    StreamCodec codec = s1.getPersistOperatorInputPort().getStreamCodec();

    assertEquals("Codec should be instance of StreamCodecWrapper", codec instanceof StreamCodecWrapperForPersistance, true);
    StreamCodecWrapperForPersistance wrapperCodec = (StreamCodecWrapperForPersistance)codec;

    Entry<InputPortMeta, Collection<PartitionKeys>> keys = (Entry<InputPortMeta, Collection<PartitionKeys>>)wrapperCodec.inputPortToPartitionMap.entrySet().iterator().next();
    logger.debug(keys.toString());
    assertEquals("Size of partitions should be 2", 2, keys.getValue().size());

    for (PTOperator ptOperator : ptos) {
      PartitioningTest.PartitionLoadWatch.put(ptOperator, -1);
      plan.onStatusUpdate(ptOperator);
    }

    dnm.processEvents();

    assertEquals("Input port map", wrapperCodec.inputPortToPartitionMap.size(), 1);

    keys = (Entry<InputPortMeta, Collection<PartitionKeys>>)wrapperCodec.inputPortToPartitionMap.entrySet().iterator().next();
    assertEquals("Size of partitions should be 1 after repartition", 1, keys.getValue().size());
    logger.debug(keys.toString());
  }
}

/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.partitioner;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.*;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.StringCodec.Object2String;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.util.TestUtils;

public class StatelessPartitionerTest
{
  @ApplicationAnnotation(name="TestApp")
  public static class DummyApp implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      DummyOperator output = dag.addOperator("DummyOutput", DummyOperator.class);
      DevNull<Integer> sink = dag.addOperator("Sink", new DevNull<Integer>());
      dag.addStream("OutputToSink", output.output, sink.data);
    }
  }

  public static class DummyOperator implements Operator
  {
    public final DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();

    private Integer value;

    public DummyOperator()
    {
    }

    public DummyOperator(Integer value)
    {
      this.value = value;
    }

    @Override
    public void beginWindow(long windowId)
    {
      //Do nothing
    }

    @Override
    public void endWindow()
    {
      //Do nothing
    }

    @Override
    public void setup(OperatorContext context)
    {
      //Do nothing
    }

    @Override
    public void teardown()
    {
      //Do nothing
    }

    public void setValue(int value)
    {
      this.value = value;
    }

    public int getValue()
    {
      return value;
    }
  }

  @Test
  public void partition1Test()
  {
    DummyOperator dummyOperator = new DummyOperator(5);
    StatelessPartitioner<DummyOperator> statelessPartitioner = new StatelessPartitioner<DummyOperator>();

    Collection<Partition<DummyOperator>> partitions = Lists.newArrayList();
    DefaultPartition<DummyOperator> defaultPartition = new DefaultPartition<DummyOperator>(dummyOperator);
    partitions.add(defaultPartition);

    Collection<Partition<DummyOperator>> newPartitions = statelessPartitioner.definePartitions(partitions, new PartitioningContextImpl(null, 0));
    Assert.assertEquals("Incorrect number of partitions", 1, newPartitions.size());

    for(Partition<DummyOperator> partition: newPartitions) {
      Assert.assertEquals("Incorrect cloned value", 5, partition.getPartitionedInstance().getValue());
    }
  }

  @Test
  public void partition5Test()
  {
    DummyOperator dummyOperator = new DummyOperator(5);
    StatelessPartitioner<DummyOperator> statelessPartitioner = new StatelessPartitioner<DummyOperator>(5);

    Collection<Partition<DummyOperator>> partitions = Lists.newArrayList();
    DefaultPartition<DummyOperator> defaultPartition = new DefaultPartition<DummyOperator>(dummyOperator);
    partitions.add(defaultPartition);

    Collection<Partition<DummyOperator>> newPartitions = statelessPartitioner.definePartitions(partitions, new PartitioningContextImpl(null, 0));
    Assert.assertEquals("Incorrect number of partitions", 5, newPartitions.size());

    for(Partition<DummyOperator> partition: newPartitions) {
      Assert.assertEquals("Incorrect cloned value", 5, partition.getPartitionedInstance().getValue());
    }
  }

  @Test
  public void objectPropertyTest()
  {
    Object2String<StatelessPartitioner<DummyOperator>> propertyReader = new Object2String<StatelessPartitioner<DummyOperator>>();
    StatelessPartitioner<DummyOperator> partitioner = propertyReader.fromString("com.datatorrent.lib.partitioner.StatelessPartitioner:3");
    Assert.assertEquals(3, partitioner.getPartitionCount());
  }

  @Test
  public void launchPartitionTestApp()
  {
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.DummyOutput.attr.PARTITIONER", "com.datatorrent.lib.partitioner.StatelessPartitioner:3");

    LocalMode lma = LocalMode.newInstance();

    try {
      lma.prepareDAG(new DummyApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(1);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Test
  public void testParallelPartitionScaleUP()
  {
    DummyOperator dummyOperator = new DummyOperator(5);
    StatelessPartitioner<DummyOperator> statelessPartitioner = new StatelessPartitioner<DummyOperator>();

    TestUtils.MockBatchedOperatorStats mockStats = new TestUtils.MockBatchedOperatorStats(2);
    mockStats.operatorStats = Lists.newArrayList();

    Collection<Partition<DummyOperator>> partitions = Lists.newArrayList();

    DefaultPartition<DummyOperator> defaultPartition = new DefaultPartition<DummyOperator>(dummyOperator);
    TestUtils.MockPartition<DummyOperator> mockPartition = new TestUtils.MockPartition<DummyOperator>(defaultPartition, mockStats);

    partitions.add(mockPartition);

    Collection<Partition<DummyOperator>> newPartitions = statelessPartitioner.definePartitions(partitions,
      new PartitioningContextImpl(null, 5));
    Assert.assertEquals("after partition", 5, newPartitions.size());
  }

  @Test
  public void testParallelPartitionScaleDown()
  {
    DummyOperator dummyOperator = new DummyOperator(5);
    StatelessPartitioner<DummyOperator> statelessPartitioner = new StatelessPartitioner<DummyOperator>();

    TestUtils.MockBatchedOperatorStats mockStats = new TestUtils.MockBatchedOperatorStats(2);
    mockStats.operatorStats = Lists.newArrayList();

    Collection<Partition<DummyOperator>> partitions = Lists.newArrayList();

    for (int i = 5; i-- > 0; ) {
      DefaultPartition<DummyOperator> defaultPartition = new DefaultPartition<DummyOperator>(dummyOperator);
      TestUtils.MockPartition<DummyOperator> mockPartition = new TestUtils.MockPartition<DummyOperator>(defaultPartition, mockStats);

      partitions.add(mockPartition);
    }

    Collection<Partition<DummyOperator>> newPartitions = statelessPartitioner.definePartitions(partitions,
      new PartitioningContextImpl(null, 1));
    Assert.assertEquals("after partition", 1, newPartitions.size());
  }

  public static class PartitioningContextImpl implements Partitioner.PartitioningContext
  {
    final int parallelPartitionCount;
    final List<InputPort<?>> ports;

    public PartitioningContextImpl(List<InputPort<?>> ports, int parallelPartitionCount)
    {
      this.ports = ports;
      this.parallelPartitionCount = parallelPartitionCount;
    }

    @Override
    public int getParallelPartitionCount()
    {
      return parallelPartitionCount;
    }

    @Override
    public List<InputPort<?>> getInputPorts()
    {
      return ports;
    }
  }

}

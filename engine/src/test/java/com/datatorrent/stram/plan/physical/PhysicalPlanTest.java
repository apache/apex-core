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
package com.datatorrent.stram.plan.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.Min;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StatsListener.StatsListenerWithContext;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.PartitioningTest;
import com.datatorrent.stram.PartitioningTest.TestInputOperator;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;
import com.datatorrent.stram.engine.GenericNodeTest;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.TestPlanContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.physical.PTOperator.PTInput;
import com.datatorrent.stram.plan.physical.PTOperator.PTOutput;
import com.datatorrent.stram.plan.physical.PhysicalPlan.LoadIndicator;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.RegexMatcher;

import static org.powermock.api.mockito.PowerMockito.mock;

public class PhysicalPlanTest
{
    /**
   * Stats listener for throughput based partitioning.
   * Used when thresholds are configured on operator through attributes.
   */
  public static class PartitionLoadWatch implements StatsListenerWithContext, java.io.Serializable
  {
    private static final Logger logger = LoggerFactory.getLogger(PartitionLoadWatch.class);
    private static final long serialVersionUID = 201312231633L;
    public long evalIntervalMillis = 30 * 1000;
    private final long tpsMin;
    private final long tpsMax;
    private long lastEvalMillis;
    private long lastTps = 0;

    private PartitionLoadWatch(long min, long max)
    {
      this.tpsMin = min;
      this.tpsMax = max;
    }

    protected LoadIndicator getLoadIndicator(int operatorId, long tps)
    {
      if ((tps < tpsMin && lastTps != 0) || tps > tpsMax) {
        lastTps = tps;
        if (tps < tpsMin) {
          return new LoadIndicator(-1, String.format("Tuples per second %d is less than the minimum %d", tps, tpsMin));
        } else {
          return new LoadIndicator(1, String.format("Tuples per second %d is greater than the maximum %d", tps, tpsMax));
        }
      }
      lastTps = tps;
      return new LoadIndicator(0, null);
    }

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      return processStats(stats, null);
    }

    @Override
    public Response processStats(BatchedOperatorStats status, StatsListenerContext context)
    {

      long tps = status.getTuplesProcessedPSMA();

      if (tps == 0L) {
        tps = status.getTuplesEmittedPSMA();
      }

      Response rsp = new Response();
      LoadIndicator loadIndicator = getLoadIndicator(status.getOperatorId(), tps);
      rsp.loadIndicator = loadIndicator.indicator;
      if (rsp.loadIndicator != 0) {
        if (lastEvalMillis < (System.currentTimeMillis() - evalIntervalMillis)) {
          lastEvalMillis = System.currentTimeMillis();
          logger.debug("Requesting repartitioning {} {}", rsp.loadIndicator, tps);
          rsp.repartitionRequired = true;
          rsp.repartitionNote = loadIndicator.note;
        }
      }
      return rsp;
    }

  }

  private static class PartitioningTestStreamCodec extends DefaultStatefulStreamCodec<Object> implements Serializable
  {
    private static final long serialVersionUID = 201410301656L;

    @Override
    public int getPartition(Object o)
    {
      return 0;
    }
  }

  public static class PartitioningTestOperator extends GenericTestOperator implements Partitioner<PartitioningTestOperator>
  {
    static final String INPORT_WITH_CODEC = "inportWithCodec";
    public Integer[] partitionKeys = {0, 1, 2};
    public String pks;
    public transient Map<Integer, Partition<PartitioningTestOperator>> partitions;
    public boolean fixedCapacity = true;
    @Min(1)
    private int partitionCount = 1;

    public PartitioningTestOperator()
    {
    }

    public void setPartitionCount(int partitionCount)
    {
      this.partitionCount = partitionCount;
    }

    public int getPartitionCount()
    {
      return partitionCount;
    }

    @InputPortFieldAnnotation(optional = true)
    public final transient InputPort<Object> inportWithCodec = new DefaultInputPort<Object>()
    {
      @Override
      public StreamCodec<Object> getStreamCodec()
      {
        return new PartitioningTestStreamCodec();
      }

      @Override
      public final void process(Object payload)
      {
      }

    };

    @Override
    public Collection<Partition<PartitioningTestOperator>> definePartitions(Collection<Partition<PartitioningTestOperator>> partitions, PartitioningContext context)
    {
      final int newPartitionCount = DefaultPartition.getRequiredPartitionCount(context, this.partitionCount);

      if (!fixedCapacity) {
        partitionKeys = new Integer[newPartitionCount];
        for (int i = 0; i < partitionKeys.length; i++) {
          partitionKeys[i] = i;
        }
      }

      List<Partition<PartitioningTestOperator>> newPartitions = new ArrayList<>(this.partitionKeys.length);
      for (Integer partitionKey : partitionKeys) {
        PartitioningTestOperator temp = new PartitioningTestOperator();
        temp.setPartitionCount(newPartitionCount);
        Partition<PartitioningTestOperator> p = new DefaultPartition<>(temp);
        PartitionKeys lpks = new PartitionKeys(2, Sets.newHashSet(partitionKey));
        p.getPartitionKeys().put(this.inport1, lpks);
        p.getPartitionKeys().put(this.inportWithCodec, lpks);
        p.getPartitionedInstance().pks = p.getPartitionKeys().values().toString();
        newPartitions.add(p);
      }

      return newPartitions;
    }

    @Override
    public void partitioned(Map<Integer, Partition<PartitioningTestOperator>> partitions)
    {
      this.partitions = partitions;
    }
  }

  @Test
  public void testStaticPartitioning()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    TestGeneratorInputOperator node0 = dag.addOperator("node0", TestGeneratorInputOperator.class);
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    PartitioningTestOperator partitioned = dag.addOperator("partitioned", PartitioningTestOperator.class);
    partitioned.setPartitionCount(partitioned.partitionKeys.length);
    GenericTestOperator singleton1 = dag.addOperator("singleton1", GenericTestOperator.class);
    GenericTestOperator singleton2 = dag.addOperator("singleton2", GenericTestOperator.class);

    dag.addStream("n0.inport1", node0.outport, node1.inport1);
    dag.addStream("n1.outport1", node1.outport1, partitioned.inport1, partitioned.inportWithCodec);
    dag.addStream("mergeStream", partitioned.outport1, singleton1.inport1, singleton2.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 2);

    OperatorMeta partitionedMeta = dag.getMeta(partitioned);

    dag.validate();

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());
    Assert.assertNotNull("partition map", partitioned.partitions);
    Assert.assertEquals("partition map " + partitioned.partitions, 3, partitioned.partitions.size());

    List<PTOperator> n2Instances = plan.getOperators(partitionedMeta);
    Assert.assertEquals("partition instances " + n2Instances, partitioned.partitionKeys.length, n2Instances.size());
    for (int i = 0; i < n2Instances.size(); i++) {
      PTOperator po = n2Instances.get(i);
      Map<String, PTInput> inputsMap = new HashMap<>();
      for (PTInput input: po.getInputs()) {
        inputsMap.put(input.portName, input);
        Assert.assertEquals("partitions " + input, Sets.newHashSet(partitioned.partitionKeys[i]), input.partitions.partitions);
        //Assert.assertEquals("codec " + input.logicalStream, PartitioningTestStreamCodec.class, input.logicalStream.getCodecClass());
      }
      Assert.assertEquals("number inputs " + inputsMap, Sets.newHashSet(PartitioningTestOperator.IPORT1, PartitioningTestOperator.INPORT_WITH_CODEC), inputsMap.keySet());
    }

    Collection<PTOperator> unifiers = plan.getMergeOperators(partitionedMeta);
    Assert.assertEquals("number unifiers " + partitionedMeta, 0, unifiers.size());
  }

  @Test
  public void testDefaultPartitioning()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.addStream("node1.outport1", node1.outport1, node2.inport2, node2.inport1);

    int initialPartitionCount = 5;
    OperatorMeta node2Decl = dag.getMeta(node2);
    node2Decl.getAttributes().put(OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(initialPartitionCount));

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());

    List<PTOperator> n2Instances = plan.getOperators(node2Decl);
    Assert.assertEquals("partition instances " + n2Instances, initialPartitionCount, n2Instances.size());

    List<Integer> assignedPartitionKeys = Lists.newArrayList();

    for (int i = 0; i < n2Instances.size(); i++) {
      PTOperator n2Partition = n2Instances.get(i);
      Assert.assertNotNull("partition keys null: " + n2Partition, n2Partition.getPartitionKeys());
      Map<InputPort<?>, PartitionKeys> pkeys = n2Partition.getPartitionKeys();
      Assert.assertEquals("partition keys size: " + pkeys, 1, pkeys.size()); // one port partitioned
      InputPort<?> expectedPort = node2.inport2;
      Assert.assertEquals("partition port: " + pkeys, expectedPort, pkeys.keySet().iterator().next());

      Assert.assertEquals("partition mask: " + pkeys, "111", Integer.toBinaryString(pkeys.get(expectedPort).mask));
      Set<Integer> pks = pkeys.get(expectedPort).partitions;
      Assert.assertTrue("number partition keys: " + pkeys, pks.size() == 1 || pks.size() == 2);
      assignedPartitionKeys.addAll(pks);
    }

    int expectedMask = Integer.parseInt("111", 2);
    Assert.assertEquals("assigned partitions ", expectedMask + 1, assignedPartitionKeys.size());
    for (int i = 0; i <= expectedMask; i++) {
      Assert.assertTrue("" + assignedPartitionKeys, assignedPartitionKeys.contains(i));
    }

  }

  @Test
  public void testNumberOfUnifiers()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.addStream("node1.outport1", node1.outport1, node2.inport1);
    dag.setOperatorAttribute(node1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(5));
    dag.setOutputPortAttribute(node1.outport1, PortContext.UNIFIER_LIMIT, 3);
    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    List<PTContainer> containers = plan.getContainers();
    int unifierCount = 0;
    int totalOperators = 0;
    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator : operators) {
        totalOperators++;
        if (operator.isUnifier()) {
          unifierCount++;
        }
      }
    }
    Assert.assertEquals("Number of operators", 8, totalOperators);
    Assert.assertEquals("Number of unifiers", 2, unifierCount);
  }

  @Test
  public void testNumberOfUnifiersWithEvenPartitions()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.addStream("node1.outport1", node1.outport1, node2.inport1);
    dag.setOperatorAttribute(node1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(8));
    dag.setOutputPortAttribute(node1.outport1, PortContext.UNIFIER_LIMIT, 4);
    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    List<PTContainer> containers = plan.getContainers();
    int unifierCount = 0;
    int totalOperators = 0;
    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator : operators) {
        totalOperators++;
        if (operator.isUnifier()) {
          unifierCount++;
        }
      }
    }
    Assert.assertEquals("Number of operators", 12, totalOperators);
    Assert.assertEquals("Number of unifiers", 3, unifierCount);
  }

  @Test
  public void testRepartitioningScaleUp()
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator mergeNode = dag.addOperator("mergeNode", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1, o2.inport2);
    dag.addStream("mergeStream", o2.outport1, mergeNode.inport1);

    OperatorMeta o2Meta = dag.getMeta(o2);
    o2Meta.getAttributes().put(OperatorContext.STATS_LISTENERS,
        Lists.newArrayList((StatsListener)new PartitionLoadWatch(0, 5)));
    o2Meta.getAttributes().put(OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(1));

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    Assert.assertEquals("number of operators", 3, plan.getAllOperators().size());
    Assert.assertEquals("number of save requests", 3, ctx.backupRequests);

    List<PTOperator> o2Partitions = plan.getOperators(o2Meta);
    Assert.assertEquals("partition count " + o2Meta, 1, o2Partitions.size());

    PTOperator o2p1 = o2Partitions.get(0);
    Assert.assertEquals("stats handlers " + o2p1, 1, o2p1.statsListeners.size());
    StatsListener sl = o2p1.statsListeners.get(0);
    Assert.assertTrue("stats handlers " + o2p1.statsListeners, sl instanceof PartitionLoadWatch);
    ((PartitionLoadWatch)sl).evalIntervalMillis = -1; // no delay

    setThroughput(o2p1, 10);
    plan.onStatusUpdate(o2p1);
    Assert.assertEquals("partitioning triggered", 1, ctx.events.size());
    ctx.backupRequests = 0;
    ctx.events.remove(0).run();

    o2Partitions = plan.getOperators(o2Meta);
    Assert.assertEquals("partition count " + o2Partitions, 2, o2Partitions.size());
    o2p1 = o2Partitions.get(0);
    Assert.assertEquals("sinks " + o2p1.getOutputs(), 1, o2p1.getOutputs().size());
    PTOperator o2p2 = o2Partitions.get(1);
    Assert.assertEquals("sinks " + o2p2.getOutputs(), 1, o2p2.getOutputs().size());

    Set<PTOperator> expUndeploy = Sets.newHashSet(plan.getOperators(dag.getMeta(mergeNode)));
    expUndeploy.add(o2p1);

    expUndeploy.addAll(plan.getOperators(dag.getMeta(mergeNode)).get(0).upstreamMerge.values());

    // verify load update generates expected events per configuration

    setThroughput(o2p1, 0);
    plan.onStatusUpdate(o2p1);
    Assert.assertEquals("load min", 0, ctx.events.size());

    setThroughput(o2p1, 3);
    plan.onStatusUpdate(o2p1);
    Assert.assertEquals("load within range", 0, ctx.events.size());

    setThroughput(o2p1, 10);
    plan.onStatusUpdate(o2p1);
    Assert.assertEquals("load exceeds max", 1, ctx.events.size());

    ctx.backupRequests = 0;
    ctx.events.remove(0).run();

    Assert.assertEquals("new partitions", 3, plan.getOperators(o2Meta).size());
    Assert.assertTrue("", plan.getOperators(o2Meta).contains(o2p2));

    for (PTOperator partition : plan.getOperators(o2Meta)) {
      Assert.assertNotNull("container null " + partition, partition.getContainer());
      Assert.assertEquals("outputs " + partition, 1, partition.getOutputs().size());
      Assert.assertEquals("downstream operators " + partition.getOutputs().get(0).sinks, 1, partition.getOutputs().get(0).sinks.size());
    }
    Assert.assertEquals("" + ctx.undeploy, expUndeploy, ctx.undeploy);

    Set<PTOperator> expDeploy = Sets.newHashSet(plan.getOperators(dag.getMeta(mergeNode)));
    expDeploy.addAll(plan.getOperators(o2Meta));
    expDeploy.remove(o2p2);

    expDeploy.addAll(plan.getOperators(dag.getMeta(mergeNode)).get(0).upstreamMerge.values());

    Assert.assertEquals("" + ctx.deploy, expDeploy, ctx.deploy);
    Assert.assertEquals("Count of storage requests", 2, ctx.backupRequests);

    // partitioning skipped on insufficient head room
    o2p1 = plan.getOperators(o2Meta).get(0);
    plan.setAvailableResources(0);
    setThroughput(o2p1, 10);
    plan.onStatusUpdate(o2p1);
    Assert.assertEquals("not repartitioned", 1, ctx.events.size());
    ctx.events.remove(0).run();
    Assert.assertEquals("partition count unchanged", 3, plan.getOperators(o2Meta).size());

  }

  /**
   * Test partitioning of an input operator (no input port).
   * Cover aspects that are not part of generic operator test.
   * Test scaling from one to multiple partitions with unifier when one partition remains unmodified.
   */
  @Test
  public void testInputOperatorPartitioning()
  {
    LogicalPlan dag = new LogicalPlan();
    final TestInputOperator<Object> o1 = dag.addOperator("o1", new TestInputOperator<>());
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.addStream("o1.outport1", o1.output, o2.inport1);

    OperatorMeta o1Meta = dag.getMeta(o1);
    dag.setOperatorAttribute(o1, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitioningTest.PartitionLoadWatch()}));
    TestPartitioner<TestInputOperator<Object>> partitioner = new TestPartitioner<>();
    dag.setOperatorAttribute(o1, OperatorContext.PARTITIONER, partitioner);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 2, plan.getContainers().size());

    List<PTOperator> o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1Partitions, 1, o1Partitions.size());
    PTOperator o1p1 = o1Partitions.get(0);

    // verify load update generates expected events per configuration
    Assert.assertEquals("stats handlers " + o1p1, 1, o1p1.statsListeners.size());
    StatsListener l = o1p1.statsListeners.get(0);
    Assert.assertTrue("stats handlers " + o1p1.statsListeners, l instanceof PartitioningTest.PartitionLoadWatch);

    PartitioningTest.PartitionLoadWatch.put(o1p1, 1);
    plan.onStatusUpdate(o1p1);
    Assert.assertEquals("scale up triggered", 1, ctx.events.size());
    // add another partition, keep existing as is
    partitioner.extraPartitions.add(new DefaultPartition<>(o1));
    Runnable r = ctx.events.remove(0);
    r.run();
    partitioner.extraPartitions.clear();

    o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("operators after scale up", 2, o1Partitions.size());
    Assert.assertEquals("first partition unmodified", o1p1, o1Partitions.get(0));
    Assert.assertEquals("single output", 1, o1p1.getOutputs().size());
    Assert.assertEquals("output to unifier", 1, o1p1.getOutputs().get(0).sinks.size());

    Set<PTOperator> expUndeploy = Sets.newHashSet(plan.getOperators(dag.getMeta(o2)));
    Set<PTOperator> expDeploy = Sets.newHashSet(o1Partitions.get(1));
    expDeploy.addAll(plan.getMergeOperators(dag.getMeta(o1)));
    expDeploy.addAll(expUndeploy);
    expDeploy.add(o1p1.getOutputs().get(0).sinks.get(0).target);

    Assert.assertEquals("undeploy", expUndeploy, ctx.undeploy);
    Assert.assertEquals("deploy", expDeploy, ctx.deploy);

    for (PTOperator p : o1Partitions) {
      Assert.assertEquals("activation window id " + p, Checkpoint.INITIAL_CHECKPOINT, p.recoveryCheckpoint);
      Assert.assertEquals("checkpoints " + p + " " + p.checkpoints, Lists.newArrayList(), p.checkpoints);
      PartitioningTest.PartitionLoadWatch.put(p, -1);
      plan.onStatusUpdate(p);
    }
    ctx.events.remove(0).run();
    Assert.assertEquals("operators after scale down", 1, plan.getOperators(o1Meta).size());
  }

  @Test
  public void testRepartitioningScaleDown()
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3parallel = dag.addOperator("o3parallel", GenericTestOperator.class);
    OperatorMeta o3Meta = dag.getMeta(o3parallel);
    GenericTestOperator mergeNode = dag.addOperator("mergeNode", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1, o2.inport2);

    dag.addStream("o2.outport1", o2.outport1, o3parallel.inport1).setLocality(Locality.CONTAINER_LOCAL);
    dag.setInputPortAttribute(o3parallel.inport1, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("o3parallel_outport1", o3parallel.outport1, mergeNode.inport1);

    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);

    OperatorMeta node2Meta = dag.getMeta(o2);
    node2Meta.getAttributes().put(OperatorContext.STATS_LISTENERS,
        Lists.newArrayList((StatsListener)new PartitionLoadWatch(3, 5)));
    node2Meta.getAttributes().put(OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(8));

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());
    Assert.assertEquals("Count of storage requests", plan.getAllOperators().size(), ctx.backupRequests);

    List<PTOperator> n2Instances = plan.getOperators(node2Meta);
    Assert.assertEquals("partition instances " + n2Instances, 8, n2Instances.size());
    PTOperator po = n2Instances.get(0);

    Collection<PTOperator> unifiers = plan.getMergeOperators(node2Meta);
    Assert.assertEquals("unifiers " + node2Meta, 0, unifiers.size());

    Collection<PTOperator> o3unifiers = plan.getOperators(dag.getMeta(mergeNode)).get(0).upstreamMerge.values();

    Assert.assertEquals("unifiers " + o3Meta, 1, o3unifiers.size());
    PTOperator o3unifier = o3unifiers.iterator().next();
    Assert.assertEquals("unifier inputs " + o3unifier, 8, o3unifier.getInputs().size());

    Set<PTOperator> expUndeploy = Sets.newHashSet(plan.getOperators(dag.getMeta(mergeNode)));
    expUndeploy.addAll(n2Instances);
    expUndeploy.addAll(plan.getOperators(o3Meta));
    expUndeploy.addAll(o3unifiers);

    // verify load update generates expected events per configuration
    Assert.assertEquals("stats handlers " + po, 1, po.statsListeners.size());
    StatsListener l = po.statsListeners.get(0);
    Assert.assertTrue("stats handlers " + po.statsListeners, l instanceof PartitionLoadWatch);

    ((PartitionLoadWatch)l).evalIntervalMillis = -1; // no delay

    setThroughput(po, 5);
    plan.onStatusUpdate(po);
    Assert.assertEquals("load upper bound", 0, ctx.events.size());

    setThroughput(po, 3);
    plan.onStatusUpdate(po);
    Assert.assertEquals("load lower bound", 0, ctx.events.size());

    setThroughput(po, 2);
    plan.onStatusUpdate(po);
    Assert.assertEquals("load below min", 1, ctx.events.size());

    ctx.backupRequests = 0;
    ctx.events.remove(0).run();

    // expect operators unchanged
    Assert.assertEquals("partitions unchanged", Sets.newHashSet(n2Instances), Sets.newHashSet(plan.getOperators(node2Meta)));

    for (PTOperator o : n2Instances) {
      setThroughput(o, 2);
      plan.onStatusUpdate(o);
    }
    Assert.assertEquals("load below min", 1, ctx.events.size());
    ctx.events.remove(0).run();
    Assert.assertEquals("partitions merged", 4, plan.getOperators(node2Meta).size());
    Assert.assertEquals("unifier inputs after scale down " + o3unifier, 4, o3unifier.getInputs().size());

    for (PTOperator p : plan.getOperators(o3Meta)) {
      Assert.assertEquals("outputs " + p.getOutputs(), 1, p.getOutputs().size());
    }

    for (PTOperator p : plan.getOperators(node2Meta)) {
      PartitionKeys pks = p.getPartitionKeys().values().iterator().next();
      Assert.assertEquals("partition mask " + p, 3, pks.mask);
      Assert.assertEquals("inputs " + p, 2, p.getInputs().size());
      boolean portConnected = false;
      for (PTInput input : p.getInputs()) {
        if (GenericTestOperator.IPORT1.equals(input.portName)) {
          portConnected = true;
          Assert.assertEquals("partition mask " + input, pks, input.partitions);
        }
      }
      Assert.assertTrue("connected " + GenericTestOperator.IPORT1, portConnected);
    }

    Assert.assertEquals("" + ctx.undeploy, expUndeploy, ctx.undeploy);

    o3unifiers = plan.getOperators(dag.getMeta(mergeNode)).get(0).upstreamMerge.values();

    Set<PTOperator> expDeploy = Sets.newHashSet(plan.getOperators(dag.getMeta(mergeNode)));
    expDeploy.addAll(plan.getOperators(node2Meta));
    expDeploy.addAll(plan.getOperators(o3Meta));
    expDeploy.addAll(o3unifiers);

    Assert.assertEquals("" + ctx.deploy, expDeploy, ctx.deploy);
    for (PTOperator oper : ctx.deploy) {
      Assert.assertNotNull("container " + oper, oper.getContainer());
    }
    Assert.assertEquals("Count of storage requests", 8, ctx.backupRequests);
  }

  /**
   * Test unifier gets removed when number partitions drops to 1.
   */
  @Test
  public void testRepartitioningScaleDownSinglePartition()
  {
    LogicalPlan dag = new LogicalPlan();

    TestInputOperator<?> o1 = dag.addOperator("o1", TestInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.output, o2.inport1);
    OperatorMeta o1Meta = dag.getMeta(o1);
    dag.setOperatorAttribute(o1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    dag.setOperatorAttribute(o1, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitioningTest.PartitionLoadWatch()}));

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    List<PTOperator> o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1Partitions, 2, o1Partitions.size());
    PTOperator o1p1 = o1Partitions.get(0);
    PTOperator p1Doper = o1p1.getOutputs().get(0).sinks.get(0).target;
    Assert.assertSame("", p1Doper.getOperatorMeta(), o1Meta.getMeta(o1.output).getUnifierMeta());
    Assert.assertTrue("unifier ", p1Doper.isUnifier());
    Assert.assertEquals("Unifiers " + o1Meta, 1, o1p1.getOutputs().get(0).sinks.size());

    Collection<PTOperator> o1Unifiers = new ArrayList<>(plan.getOperators(dag.getMeta(o2)).get(0).upstreamMerge.values());

    StatsListener l = o1p1.statsListeners.get(0);
    Assert.assertTrue("stats handlers " + o1p1.statsListeners, l instanceof PartitioningTest.PartitionLoadWatch);
    PartitioningTest.PartitionLoadWatch.put(o1p1, -1);
    PartitioningTest.PartitionLoadWatch.put(o1Partitions.get(1), -1);

    plan.onStatusUpdate(o1p1);
    plan.onStatusUpdate(o1Partitions.get(1));
    Assert.assertEquals("partition scaling triggered", 1, ctx.events.size());
    ctx.events.remove(0).run();

    List<PTOperator> o1NewPartitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1NewPartitions, 1, o1NewPartitions.size());

    List<PTOperator> o1NewUnifiers = new ArrayList<>(plan.getOperators(dag.getMeta(o2)).get(0).upstreamMerge.values());

    Assert.assertEquals("unifiers " + o1Meta, 0, o1NewUnifiers.size());
    p1Doper = o1p1.getOutputs().get(0).sinks.get(0).target;
    Assert.assertTrue("", p1Doper.getOperatorMeta() == dag.getMeta(o2));
    Assert.assertFalse("unifier ", p1Doper.isUnifier());

    Assert.assertTrue("removed unifier from deployment " + ctx.undeploy, ctx.undeploy.containsAll(o1Unifiers));
    Assert.assertFalse("removed unifier from deployment " + ctx.deploy, ctx.deploy.containsAll(o1Unifiers));

    // scale up, ensure unifier is setup at activation checkpoint
    setActivationCheckpoint(o1NewPartitions.get(0), 3);
    PartitioningTest.PartitionLoadWatch.put(o1NewPartitions.get(0), 1);
    plan.onStatusUpdate(o1NewPartitions.get(0));
    Assert.assertEquals("partition scaling triggered", 1, ctx.events.size());
    ctx.events.remove(0).run();

    o1NewUnifiers.addAll(plan.getOperators(dag.getMeta(o2)).get(0).upstreamMerge.values());

    Assert.assertEquals("unifiers " + o1Meta, 1, o1NewUnifiers.size());
    Assert.assertEquals("unifier activation checkpoint " + o1Meta, 3, o1NewUnifiers.get(0).recoveryCheckpoint.windowId);
  }

  public static void setActivationCheckpoint(PTOperator oper, long windowId)
  {
    try {
      oper.operatorMeta.getValue(OperatorContext.STORAGE_AGENT).save(oper.operatorMeta.getOperator(), oper.id, windowId);
      Checkpoint cp = new Checkpoint(windowId, 0, 0);
      oper.setRecoveryCheckpoint(cp);
      oper.checkpoints.add(cp);
    } catch (Exception e) {
      Assert.fail(e.toString());
    }
  }

  @Test
  public void testDefaultRepartitioning()
  {

    List<PartitionKeys> twoBitPartitionKeys = Arrays.asList(
        newPartitionKeys("11", "00"),
        newPartitionKeys("11", "10"),
        newPartitionKeys("11", "01"),
        newPartitionKeys("11", "11"));

    GenericTestOperator operator = new GenericTestOperator();

    Set<PartitionKeys> initialPartitionKeys = Sets.newHashSet(
        newPartitionKeys("1", "0"),
        newPartitionKeys("1", "1"));

    final ArrayList<Partition<Operator>> partitions = new ArrayList<>();
    for (PartitionKeys pks : initialPartitionKeys) {
      Map<InputPort<?>, PartitionKeys> p1Keys = new HashMap<>();
      p1Keys.put(operator.inport1, pks);
      partitions.add(new DefaultPartition<Operator>(operator, p1Keys, 1, null));
    }

    ArrayList<Partition<Operator>> lowLoadPartitions = new ArrayList<>();
    for (Partition<Operator> p : partitions) {
      lowLoadPartitions.add(new DefaultPartition<>(p.getPartitionedInstance(), p.getPartitionKeys(), -1, null));
    }
    // merge to single partition
    List<Partition<Operator>> newPartitions = Lists.newArrayList();
    Collection<Partition<Operator>> tempNewPartitions = StatelessPartitioner.repartition(lowLoadPartitions);
    newPartitions.addAll(tempNewPartitions);
    Assert.assertEquals("" + newPartitions, 1, newPartitions.size());
    Assert.assertEquals("" + newPartitions.get(0).getPartitionKeys(), 0, newPartitions.get(0).getPartitionKeys().values().iterator().next().mask);

    List<Partition<Operator>> tempList = Collections.singletonList((Partition<Operator>)new DefaultPartition<Operator>(operator, newPartitions.get(0).getPartitionKeys(), -1, null));
    tempNewPartitions = StatelessPartitioner.repartition(tempList);
    newPartitions.clear();
    newPartitions.addAll(tempNewPartitions);
    Assert.assertEquals("" + newPartitions, 1, newPartitions.size());

    // split back into two
    tempList = Collections.singletonList((Partition<Operator>)new DefaultPartition<Operator>(operator, newPartitions.get(0).getPartitionKeys(), 1, null));
    tempNewPartitions = StatelessPartitioner.repartition(tempList);
    newPartitions.clear();
    newPartitions.addAll(tempNewPartitions);
    Assert.assertEquals("" + newPartitions, 2, newPartitions.size());

    // split partitions
    tempNewPartitions = StatelessPartitioner.repartition(partitions);
    newPartitions.clear();
    newPartitions.addAll(tempNewPartitions);
    Assert.assertEquals("" + newPartitions, 4, newPartitions.size());

    Set<PartitionKeys> expectedPartitionKeys = Sets.newHashSet(twoBitPartitionKeys);
    for (Partition<?> p: newPartitions) {
      Assert.assertEquals("" + p.getPartitionKeys(), 1, p.getPartitionKeys().size());
      Assert.assertEquals("" + p.getPartitionKeys(), operator.inport1, p.getPartitionKeys().keySet().iterator().next());
      PartitionKeys pks = p.getPartitionKeys().values().iterator().next();
      expectedPartitionKeys.remove(pks);
    }
    Assert.assertTrue("" + expectedPartitionKeys, expectedPartitionKeys.isEmpty());

    // partition merge
    List<HashSet<PartitionKeys>> expectedKeysSets = Arrays.asList(
        Sets.newHashSet(newPartitionKeys("11", "00"), newPartitionKeys("11", "10"), newPartitionKeys("1", "1")),
        Sets.newHashSet(newPartitionKeys("1", "0"), newPartitionKeys("11", "01"), newPartitionKeys("11", "11"))
    );

    for (Set<PartitionKeys> expectedKeys: expectedKeysSets) {
      List<Partition<Operator>> clonePartitions = Lists.newArrayList();
      for (PartitionKeys pks: twoBitPartitionKeys) {
        Map<InputPort<?>, PartitionKeys> p1Keys = new HashMap<>();
        p1Keys.put(operator.inport1, pks);
        int load = expectedKeys.contains(pks) ? 0 : -1;
        clonePartitions.add(new DefaultPartition<Operator>(operator, p1Keys, load, null));
      }

      tempNewPartitions = StatelessPartitioner.repartition(clonePartitions);
      newPartitions.clear();
      newPartitions.addAll(tempNewPartitions);
      Assert.assertEquals("" + newPartitions, 3, newPartitions.size());

      for (Partition<?> p: newPartitions) {
        Assert.assertEquals("" + p.getPartitionKeys(), 1, p.getPartitionKeys().size());
        Assert.assertEquals("" + p.getPartitionKeys(), operator.inport1, p.getPartitionKeys().keySet().iterator().next());
        PartitionKeys pks = p.getPartitionKeys().values().iterator().next();
        expectedKeys.remove(pks);
      }
      Assert.assertTrue("" + expectedKeys, expectedKeys.isEmpty());
    }

    // merge 2 into single partition
    lowLoadPartitions = Lists.newArrayList();
    for (Partition<?> p : partitions) {
      lowLoadPartitions.add(new DefaultPartition<Operator>(operator, p.getPartitionKeys(), -1, null));
    }
    tempNewPartitions = StatelessPartitioner.repartition(lowLoadPartitions);
    newPartitions.clear();
    newPartitions.addAll(tempNewPartitions);
    Assert.assertEquals("" + newPartitions, 1, newPartitions.size());
    for (Partition<?> p: newPartitions) {
      Assert.assertEquals("" + p.getPartitionKeys(), 1, p.getPartitionKeys().size());
      PartitionKeys pks = p.getPartitionKeys().values().iterator().next();
      Assert.assertEquals("" + pks, 0, pks.mask);
      Assert.assertEquals("" + pks, Sets.newHashSet(0), pks.partitions);
    }
  }

  private PartitionKeys newPartitionKeys(String mask, String key)
  {
    return new PartitionKeys(Integer.parseInt(mask, 2), Sets.newHashSet(Integer.parseInt(key, 2)));
  }

  private void setThroughput(PTOperator oper, long tps)
  {
    oper.stats.statsRevs.checkout();
    oper.stats.tuplesProcessedPSMA.set(tps);
    oper.stats.statsRevs.commit();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInline()
  {

    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    PartitioningTestOperator partOperator = dag.addOperator("partNode", PartitioningTestOperator.class);
    partOperator.partitionKeys = new Integer[] {0,1};
    dag.getMeta(partOperator).getAttributes().put(OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(partOperator.partitionKeys.length));

    dag.addStream("o1_outport1", o1.outport1, o2.inport1, o3.inport1, partOperator.inport1)
            .setLocality(null);

    // same container for o2 and o3
    dag.addStream("o2_outport1", o2.outport1, o3.inport2)
        .setLocality(Locality.CONTAINER_LOCAL);

    dag.addStream("o3_outport1", o3.outport1, partOperator.inport2);

    int maxContainers = 4;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new TestPlanContext());

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", maxContainers, plan.getContainers().size());
    Assert.assertEquals("operators container 0", 1, plan.getContainers().get(0).getOperators().size());

    Assert.assertEquals("operators container 0", 1, plan.getContainers().get(0).getOperators().size());
    Set<OperatorMeta> c2ExpNodes = Sets.newHashSet(dag.getMeta(o2), dag.getMeta(o3));
    Set<OperatorMeta> c2ActNodes = new HashSet<>();
    PTContainer c2 = plan.getContainers().get(1);
    for (PTOperator pNode : c2.getOperators()) {
      c2ActNodes.add(pNode.getOperatorMeta());
    }
    Assert.assertEquals("operators " + c2, c2ExpNodes, c2ActNodes);

    // one container per partition
    OperatorMeta partOperMeta = dag.getMeta(partOperator);
    List<PTOperator> partitions = plan.getOperators(partOperMeta);
    for (PTOperator partition : partitions) {
      Assert.assertEquals("operators container" + partition, 1, partition.getContainer().getOperators().size());
    }

  }

  @Test
  public void testInlineMultipleInputs()
  {

    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);

    dag.addStream("n1Output1", node1.outport1, node3.inport1)
        .setLocality(Locality.CONTAINER_LOCAL);

    dag.addStream("n2Output1", node2.outport1, node3.inport2)
        .setLocality(Locality.CONTAINER_LOCAL);

    int maxContainers = 5;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new TestPlanContext());

    PhysicalPlan deployer = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", 1, deployer.getContainers().size());

    PTOutput node1Out = deployer.getOperators(dag.getMeta(node1)).get(0).getOutputs().get(0);
    Assert.assertTrue("inline " + node1Out, node1Out.isDownStreamInline());

    // per current logic, different container is assigned to second input node
    PTOutput node2Out = deployer.getOperators(dag.getMeta(node2)).get(0).getOutputs().get(0);
    Assert.assertTrue("inline " + node2Out, node2Out.isDownStreamInline());

  }

  @Test
  public void testNodeLocality()
  {

    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    dag.getMeta(partitioned).getAttributes().put(OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));

    GenericTestOperator partitionedParallel = dag.addOperator("partitionedParallel", GenericTestOperator.class);

    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1).setLocality(null);

    dag.addStream("partitioned_outport1", partitioned.outport1, partitionedParallel.inport2).setLocality(Locality.NODE_LOCAL);
    dag.setInputPortAttribute(partitionedParallel.inport2, PortContext.PARTITION_PARALLEL, true);

    GenericTestOperator single = dag.addOperator("single", GenericTestOperator.class);
    dag.addStream("partitionedParallel_outport1", partitionedParallel.outport1, single.inport1);

    int maxContainers = 6;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new TestPlanContext());

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", maxContainers, plan.getContainers().size());

    PTContainer container1 = plan.getContainers().get(0);
    Assert.assertEquals("number operators " + container1, 1, container1.getOperators().size());
    Assert.assertEquals("operators " + container1, dag.getMeta(o1), container1.getOperators().get(0).getOperatorMeta());

    for (int i = 1; i < 3; i++) {
      PTContainer c = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + c, 1, c.getOperators().size());
      Set<OperatorMeta> expectedLogical = Sets.newHashSet(dag.getMeta(partitioned));
      Set<OperatorMeta> actualLogical = Sets.newHashSet();
      for (PTOperator p : c.getOperators()) {
        actualLogical.add(p.getOperatorMeta());
      }
      Assert.assertEquals("operators " + c, expectedLogical, actualLogical);
    }
    // in-node parallel partition
    for (int i = 3; i < 5; i++) {
      PTContainer c = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + c, 1, c.getOperators().size());
      Set<OperatorMeta> expectedLogical = Sets.newHashSet(dag.getMeta(partitionedParallel));
      Set<OperatorMeta> actualLogical = Sets.newHashSet();
      for (PTOperator p : c.getOperators()) {
        actualLogical.add(p.getOperatorMeta());
        Assert.assertEquals("nodeLocal " + p.getNodeLocalOperators(), 2, p.getNodeLocalOperators().getOperatorSet()
            .size());
      }
      Assert.assertEquals("operators " + c, expectedLogical, actualLogical);
    }
  }

  @Test
  public void testParallelPartitioning()
  {

    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));

    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("o1Output1", o1.outport1, o2.inport1, o3.inport1).setLocality(null);

    dag.addStream("o2Output1", o2.outport1, o3.inport2).setLocality(Locality.CONTAINER_LOCAL);
    dag.setInputPortAttribute(o3.inport2, PortContext.PARTITION_PARALLEL, true);

    // parallel partition two downstream operators
    PartitioningTestOperator o3_1 = dag.addOperator("o3_1", PartitioningTestOperator.class);
    o3_1.fixedCapacity = false;
    dag.setInputPortAttribute(o3_1.inport1, PortContext.PARTITION_PARALLEL, true);
    OperatorMeta o3_1Meta = dag.getMeta(o3_1);

    GenericTestOperator o3_2 = dag.addOperator("o3_2", GenericTestOperator.class);
    dag.setInputPortAttribute(o3_2.inport1, PortContext.PARTITION_PARALLEL, true);
    OperatorMeta o3_2Meta = dag.getMeta(o3_2);

    dag.addStream("o3outport1", o3.outport1, o3_1.inport1, o3_2.inport1).setLocality(Locality.CONTAINER_LOCAL);

    // join within parallel partition
    GenericTestOperator o4 = dag.addOperator("o4", GenericTestOperator.class);
    dag.setInputPortAttribute(o4.inport1, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(o4.inport2, PortContext.PARTITION_PARALLEL, true);
    OperatorMeta o4Meta = dag.getMeta(o4);

    dag.addStream("o3_1.outport1", o3_1.outport1, o4.inport1).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("o3_2.outport1", o3_2.outport1, o4.inport2).setLocality(Locality.CONTAINER_LOCAL);

    // non inline
    GenericTestOperator o5single = dag.addOperator("o5single", GenericTestOperator.class);
    dag.addStream("o4outport1", o4.outport1, o5single.inport1);

    int maxContainers = 4;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new TestPlanContext());

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", 4, plan.getContainers().size());

    PTContainer container1 = plan.getContainers().get(0);
    Assert.assertEquals("number operators " + container1, 1, container1.getOperators().size());
    Assert.assertEquals("operators " + container1, "o1", container1.getOperators().get(0).getOperatorMeta().getName());

    for (int i = 1; i < 3; i++) {
      PTContainer container2 = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container2, 5, container2.getOperators().size());
      Set<String> expectedLogicalNames = Sets.newHashSet("o2", "o3", o3_1Meta.getName(), o3_2Meta.getName(), o4Meta.getName());
      Set<String> actualLogicalNames = Sets.newHashSet();
      for (PTOperator p: container2.getOperators()) {
        actualLogicalNames.add(p.getOperatorMeta().getName());
      }
      Assert.assertEquals("operator names " + container2, expectedLogicalNames, actualLogicalNames);
    }

    List<OperatorMeta> inlineOperators = Lists.newArrayList(dag.getMeta(o2), o3_1Meta, o3_2Meta);
    for (OperatorMeta ow: inlineOperators) {
      List<PTOperator> partitionedInstances = plan.getOperators(ow);
      Assert.assertEquals("" + partitionedInstances, 2, partitionedInstances.size());
      for (PTOperator p: partitionedInstances) {
        Assert.assertEquals("outputs " + p, 1, p.getOutputs().size());
        Assert.assertTrue("downstream inline " + p.getOutputs().get(0), p.getOutputs().get(0).isDownStreamInline());
      }
    }

    // container 4: Unifier for o4 & O5
    PTContainer container4 = plan.getContainers().get(3);

    PTOperator ptOperatorO5 = plan.getOperators(dag.getMeta(o5single)).get(0);
    PTOperator unifier = ptOperatorO5.upstreamMerge.values().iterator().next();

    Assert.assertEquals("number operators " + container4, 2, container4.getOperators().size());
    Assert.assertEquals("operators " + container4, o4Meta.getMeta(o4.outport1).getUnifierMeta(), unifier.getOperatorMeta());

    Assert.assertEquals("unifier inputs" + unifier.getInputs(), 2, unifier.getInputs().size());
    Assert.assertEquals("unifier outputs" + unifier.getOutputs(), 1, unifier.getOutputs().size());

    OperatorMeta o5Meta = dag.getMeta(o5single);
    Assert.assertEquals("operators " + container4, o5Meta, ptOperatorO5.getOperatorMeta());
    List<PTOperator> o5Instances = plan.getOperators(o5Meta);
    Assert.assertEquals("" + o5Instances, 1, o5Instances.size());
    Assert.assertEquals("inputs" + ptOperatorO5.getInputs(), 1, ptOperatorO5.getInputs().size());
    Assert.assertEquals("inputs" + ptOperatorO5.getInputs(), unifier, ptOperatorO5.getInputs().get(0).source.source);

    // verify partitioner was called for parallel partition
    Assert.assertNotNull("partitioner called " + o3_1, o3_1.partitions);
    for (PTOperator p : plan.getOperators(o3_1Meta)) {
      Assert.assertEquals("inputs " + p, 1, p.getInputs().size());
      for (PTInput pti : p.getInputs()) {
        Assert.assertNull("partition keys " + pti, pti.partitions);
      }
    }

  }

  @Test
  public void testParallelPartitioningValidation()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new TestPlanContext());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    dag.setInputPortAttribute(o3.inport1, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(o3.inport2, PortContext.PARTITION_PARALLEL, true);

    dag.addStream("o1Output1", o1.outport1, o3.inport1);
    dag.addStream("o2Output1", o2.outport1, o3.inport2);

    try {
      new PhysicalPlan(dag, new TestPlanContext());
    } catch (AssertionError ae) {
      Assert.assertThat("Parallel partition needs common ancestor", ae.getMessage(), RegexMatcher.matches("operator cannot extend multiple partitions.*"));
    }

    GenericTestOperator commonAncestor = dag.addOperator("commonAncestor", GenericTestOperator.class);
    dag.setInputPortAttribute(o1.inport1, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(o2.inport1, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("commonAncestor.outport1", commonAncestor.outport1, o1.inport1);
    dag.addStream("commonAncestor.outport2", commonAncestor.outport2, o2.inport1);
    new PhysicalPlan(dag, new TestPlanContext());

    // two operators with multiple streams
    dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new TestPlanContext());
    o1 = dag.addOperator("o1", GenericTestOperator.class);
    o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setInputPortAttribute(o2.inport1, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(o2.inport2, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    dag.addStream("o2.outport2", o1.outport2, o2.inport2);
    new PhysicalPlan(dag, new TestPlanContext());
  }

  /**
   * MxN partitioning. When source and sink of a stream are partitioned, a
   * separate unifier is created container local with each downstream partition.
   */
  @Test
  public void testMxNPartitioning()
  {

    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    TestPartitioner<TestGeneratorInputOperator> o1Partitioner = new TestPartitioner<>();
    o1Partitioner.setPartitionCount(2);
    dag.setOperatorAttribute(o1, OperatorContext.PARTITIONER, o1Partitioner);
    dag.setOperatorAttribute(o1, OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)new PartitioningTest.PartitionLoadWatch()));
    OperatorMeta o1Meta = dag.getMeta(o1);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<TestGeneratorInputOperator>(3));
    dag.setOperatorAttribute(o2, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitioningTest.PartitionLoadWatch()}));
    OperatorMeta o2Meta = dag.getMeta(o2);

    dag.addStream("o1.outport1", o1.outport, o2.inport1);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);

    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 5, plan.getContainers().size());

    List<PTOperator> inputOperators = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertEquals("operators " + container, o1Meta.getName(), container.getOperators().get(0).getOperatorMeta().getName());
      inputOperators.add(container.getOperators().get(0));
    }

    for (int i = 2; i < 5; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 2, container.getOperators().size());
      Assert.assertEquals("operators " + container, o2Meta.getName(), container.getOperators().get(0).getOperatorMeta().getName());
      Set<String> expectedLogicalNames = Sets.newHashSet(o1Meta.getMeta(o1.outport).getUnifierMeta().getName(), o2Meta.getName());
      Map<String, PTOperator> actualOperators = new HashMap<>();
      for (PTOperator p : container.getOperators()) {
        actualOperators.put(p.getOperatorMeta().getName(), p);
      }
      Assert.assertEquals("", expectedLogicalNames, actualOperators.keySet());

      PTOperator pUnifier = actualOperators.get(o1Meta.getMeta(o1.outport).getUnifierMeta().getName());
      Assert.assertNotNull("" + pUnifier, pUnifier.getContainer());
      Assert.assertTrue("" + pUnifier, pUnifier.isUnifier());
      // input from each upstream partition
      Assert.assertEquals("" + pUnifier, 2, pUnifier.getInputs().size());
      int numberPartitionKeys = (i == 2) ? 2 : 1;
      for (int inputIndex = 0; inputIndex < pUnifier.getInputs().size(); inputIndex++) {
        PTInput input = pUnifier.getInputs().get(inputIndex);
        Assert.assertEquals("" + pUnifier, "outport", input.source.portName);
        Assert.assertEquals("" + pUnifier, inputOperators.get(inputIndex), input.source.source);
        Assert.assertEquals("partition keys " + input.partitions, numberPartitionKeys, input.partitions.partitions.size());
      }
      // output to single downstream partition
      Assert.assertEquals("" + pUnifier, 1, pUnifier.getOutputs().size());
      Assert.assertTrue("" + actualOperators.get(o2Meta.getName()).getOperatorMeta().getOperator(),
          actualOperators.get(o2Meta.getName()).getOperatorMeta().getOperator() instanceof GenericTestOperator);

      PTOperator p = actualOperators.get(o2Meta.getName());
      Assert.assertEquals("partition inputs " + p.getInputs(), 1, p.getInputs().size());
      Assert.assertEquals("partition inputs " + p.getInputs(), pUnifier, p.getInputs().get(0).source.source);
      Assert.assertEquals("input partition keys " + p.getInputs(), null, p.getInputs().get(0).partitions);
      Assert.assertTrue("partitioned unifier container local " + p.getInputs().get(0).source, p.getInputs().get(0).source.isDownStreamInline());
    }

    // Test Dynamic change
    // for M x N partition
    // scale down N from 3 to 2 and then from 2 to 1
    for (int i = 0; i < 2; i++) {
      List<PTOperator> ptos = plan.getOperators(o2Meta);
      Set<PTOperator> expUndeploy = Sets.newHashSet(ptos);
      for (PTOperator ptOperator : ptos) {
        expUndeploy.addAll(ptOperator.upstreamMerge.values());
        PartitioningTest.PartitionLoadWatch.put(ptOperator, -1);
        plan.onStatusUpdate(ptOperator);
      }
      ctx.backupRequests = 0;
      ctx.events.remove(0).run();
      Set<PTOperator> expDeploy = Sets.newHashSet(plan.getOperators(o2Meta));
      // Either unifiers for each partition or single unifier for single partition is expected to be deployed
      expDeploy.addAll(plan.getMergeOperators(o1Meta));
      for (PTOperator ptOperator : plan.getOperators(o2Meta)) {
        expDeploy.addAll(ptOperator.upstreamMerge.values());
      }

      Assert.assertEquals("number of containers", 4 - i, plan.getContainers().size());
      Assert.assertEquals("number of operators", 2 - i, plan.getOperators(o2Meta).size());
      Assert.assertEquals("undeployed operators " + ctx.undeploy, expUndeploy, ctx.undeploy);
      Assert.assertEquals("deployed operators " + ctx.deploy, expDeploy, ctx.deploy);
    }

    // scale up N from 1 to 2 and then from 2 to 3
    for (int i = 0; i < 2; i++) {

      List<PTOperator> unChangedOps = new LinkedList<>(plan.getOperators(o2Meta));
      PTOperator o2p1 = unChangedOps.remove(0);
      Set<PTOperator> expUndeploy = Sets.newHashSet(o2p1);
      // Either single unifier for one partition or merged unifiers for each partition is expected to be undeployed
      expUndeploy.addAll(plan.getMergeOperators(o1Meta));
      expUndeploy.addAll(o2p1.upstreamMerge.values());
      List<PTOperator> nOps = new LinkedList<>();
      for (Iterator<PTOperator> iterator = unChangedOps.iterator(); iterator.hasNext(); ) {
        PTOperator ptOperator = iterator.next();
        nOps.addAll(ptOperator.upstreamMerge.values());
      }
      unChangedOps.addAll(nOps);

      PartitioningTest.PartitionLoadWatch.put(o2p1, 1);

      plan.onStatusUpdate(o2p1);
      Assert.assertEquals("repartition event", 1, ctx.events.size());
      ctx.backupRequests = 0;
      ctx.events.remove(0).run();

      Assert.assertEquals("N partitions after scale up " + o2Meta, 2 + i, plan.getOperators(o2Meta).size());
      Assert.assertTrue("no unifiers", plan.getMergeOperators(o1Meta).isEmpty());

      for (PTOperator o : plan.getOperators(o2Meta)) {
        Assert.assertNotNull(o.container);
        PTOperator unifier = o.upstreamMerge.values().iterator().next();
        Assert.assertNotNull(unifier.container);
        Assert.assertSame("unifier in same container", o.container, unifier.container);
        Assert.assertEquals("container operators " + o.container, Sets.newHashSet(o.container.getOperators()), Sets.newHashSet(o, unifier));
      }
      Set<PTOperator> expDeploy = Sets.newHashSet(plan.getOperators(o2Meta));
      for (PTOperator ptOperator : plan.getOperators(o2Meta)) {
        expDeploy.addAll(ptOperator.upstreamMerge.values());
      }
      expDeploy.removeAll(unChangedOps);
      Assert.assertEquals("number of containers", 4 + i, plan.getContainers().size());
      Assert.assertEquals("undeployed operators" + ctx.undeploy, expUndeploy, ctx.undeploy);
      Assert.assertEquals("deployed operators" + ctx.deploy, expDeploy, ctx.deploy);

    }

    // scale down M to 1
    {
      Set<PTOperator> expUndeploy = Sets.newHashSet();
      Set<PTOperator> expDeploy = Sets.newHashSet();
      for (PTOperator o2p : plan.getOperators(o2Meta)) {
        expUndeploy.addAll(o2p.upstreamMerge.values());
        expUndeploy.add(o2p);
        expDeploy.add(o2p);
      }

      for (PTOperator o1p : plan.getOperators(o1Meta)) {
        expUndeploy.add(o1p);
        PartitioningTest.PartitionLoadWatch.put(o1p, -1);
        plan.onStatusUpdate(o1p);
      }

      Assert.assertEquals("repartition event", 1, ctx.events.size());
      ctx.events.remove(0).run();

      Assert.assertEquals("M partitions after scale down " + o1Meta, 1, plan.getOperators(o1Meta).size());
      expUndeploy.removeAll(plan.getOperators(o1Meta));

      for (PTOperator o2p : plan.getOperators(o2Meta)) {
        Assert.assertTrue("merge unifier " + o2p + " " + o2p.upstreamMerge, o2p.upstreamMerge.isEmpty());
      }

      Assert.assertEquals("undeploy", expUndeploy, ctx.undeploy);
      Assert.assertEquals("deploy", expDeploy, ctx.deploy);
    }

    // scale up M to 2
    Assert.assertEquals("M partitions " + o1Meta, 1, plan.getOperators(o1Meta).size());
    {
      Set<PTOperator> expUndeploy = Sets.newHashSet();
      Set<PTOperator> expDeploy = Sets.newHashSet();
      for (PTOperator o1p : plan.getOperators(o1Meta)) {
        PartitioningTest.PartitionLoadWatch.put(o1p, 1);
        plan.onStatusUpdate(o1p);
      }

      Assert.assertEquals("repartition event", 1, ctx.events.size());
      o1Partitioner.extraPartitions.add(new DefaultPartition<>(o1));
      ctx.events.remove(0).run();
      o1Partitioner.extraPartitions.clear();

      List<PTOperator> o1Partitions = plan.getOperators(o1Meta);
      List<PTOperator> o2Partitions = plan.getOperators(o2Meta);
      Assert.assertEquals("M partitions after scale up " + o1Meta, 2, o1Partitions.size());
      expDeploy.add(o1Partitions.get(1)); // previous partition unchanged
      for (PTOperator o1p : o1Partitions) {
        Assert.assertEquals("outputs " + o1p, 1, o1p.getOutputs().size());
        Assert.assertEquals("sinks " + o1p, o2Partitions.size(), o1p.getOutputs().get(0).sinks.size());
      }
      for (PTOperator o2p : plan.getOperators(o2Meta)) {
        expUndeploy.add(o2p);
        expDeploy.add(o2p);
        Assert.assertEquals("merge unifier " + o2p + " " + o2p.upstreamMerge, 1, o2p.upstreamMerge.size());
        expDeploy.addAll(o2p.upstreamMerge.values());
      }
      Assert.assertEquals("undeploy", expUndeploy, ctx.undeploy);
      Assert.assertEquals("deploy", expDeploy, ctx.deploy);
    }

  }

  /**
   * MxN partitioning. When source and sink of a stream are partitioned, a
   * separate unifier is created container local with each downstream partition.
   */
  @Test
  public void testSingleFinalMxNPartitioning()
  {

    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.PARTITIONER, new StatelessPartitioner<TestGeneratorInputOperator>(2));
    dag.setOperatorAttribute(o1, OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)new PartitioningTest.PartitionLoadWatch()));
    dag.setOutputPortAttribute(o1.outport, PortContext.UNIFIER_SINGLE_FINAL, true);
    OperatorMeta o1Meta = dag.getMeta(o1);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<TestGeneratorInputOperator>(3));
    dag.setOperatorAttribute(o2, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitioningTest.PartitionLoadWatch()}));
    OperatorMeta o2Meta = dag.getMeta(o2);

    dag.addStream("o1.outport1", o1.outport, o2.inport1);

    int maxContainers = 10;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);

    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 6, plan.getContainers().size());

    List<PTOperator> inputOperators = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertEquals("operators " + container, o1Meta.getName(), container.getOperators().get(0).getOperatorMeta().getName());
      inputOperators.add(container.getOperators().get(0));
    }

    PTOperator inputUnifier = null;
    {
      PTContainer container = plan.getContainers().get(2);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      PTOperator pUnifier = container.getOperators().get(0);
      Assert.assertEquals("operators " + container, o1Meta.getMeta(o1.outport).getUnifierMeta().getName(), pUnifier.getOperatorMeta().getName());
      Assert.assertTrue("single unifier " + pUnifier, pUnifier.isUnifier());
      Assert.assertEquals("" + pUnifier, 2, pUnifier.getInputs().size());
      for (int inputIndex = 0; inputIndex < pUnifier.getInputs().size(); inputIndex++) {
        PTInput input = pUnifier.getInputs().get(inputIndex);
        Assert.assertEquals("source port name " + pUnifier, "outport", input.source.portName);
        Assert.assertEquals("" + pUnifier, inputOperators.get(inputIndex), input.source.source);
        Assert.assertEquals("partition keys " + input.partitions, null, input.partitions);
      }
      Assert.assertEquals("number outputs " + pUnifier, 1, pUnifier.getOutputs().size());
      PTOutput output = pUnifier.getOutputs().get(0);
      Assert.assertEquals("number inputs " + output, 3, output.sinks.size());
      for (int inputIndex = 0; inputIndex < output.sinks.size(); ++inputIndex) {
        Assert.assertEquals("output sink " + output, o2Meta.getName(), output.sinks.get(inputIndex).target.getName());
        Assert.assertEquals("destination port name " + output, GenericTestOperator.IPORT1, output.sinks.get(inputIndex).portName);
      }
      inputUnifier = pUnifier;
    }

    List<Integer> partitionKeySizes = new ArrayList<>();
    for (int i = 3; i < 6; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertEquals("operators " + container, o2Meta.getName(), container.getOperators().get(0).getOperatorMeta().getName());

      PTOperator operator = container.getOperators().get(0);
      Assert.assertEquals("operators " + container, o2Meta.getName(), operator.getOperatorMeta().getName());
      Assert.assertEquals("number inputs " + operator, 1, operator.getInputs().size());
      PTInput input = operator.getInputs().get(0);
      Assert.assertEquals("" + operator, inputUnifier, input.source.source);
      Assert.assertNotNull("input partitions " + operator, input.partitions);
      partitionKeySizes.add(input.partitions.partitions.size());
    }

    Assert.assertEquals("input partition sizes count", 3, partitionKeySizes.size());
    Collections.sort(partitionKeySizes);
    Assert.assertEquals("input partition sizes", Arrays.asList(1, 1, 2), partitionKeySizes);

    // Test Dynamic change
    // for M x N partition
    // scale down N from 3 to 2 and then from 2 to 1
    for (int i = 0; i < 2; i++) {
      List<PTOperator> ptos =  plan.getOperators(o2Meta);
      Set<PTOperator> expUndeploy = Sets.newHashSet(ptos);
      for (PTOperator ptOperator : ptos) {
        //expUndeploy.addAll(ptOperator.upstreamMerge.values());
        expUndeploy.add(ptOperator);
        PartitioningTest.PartitionLoadWatch.put(ptOperator, -1);
        plan.onStatusUpdate(ptOperator);
      }
      ctx.backupRequests = 0;
      ctx.events.remove(0).run();
      Assert.assertEquals("single unifier ", 1, plan.getMergeOperators(o1Meta).size());
      Set<PTOperator> expDeploy = Sets.newHashSet(plan.getOperators(o2Meta));
      // The unifier and o2 operators are expected to be deployed because of partition key changes
      for (PTOperator ptOperator : plan.getOperators(o2Meta)) {
        expDeploy.add(ptOperator);
      }
      // from 3 to 2 the containers decrease from 5 to 4, but from 2 to 1 the container remains same because single unifier are not inline with single operator partition
      Assert.assertEquals("number of containers", 5 - i, plan.getContainers().size());
      Assert.assertEquals("number of operators", 2 - i, plan.getOperators(o2Meta).size());
      Assert.assertEquals("undeployed operators " + ctx.undeploy, expUndeploy, ctx.undeploy);
      Assert.assertEquals("deployed operators " + ctx.deploy, expDeploy, ctx.deploy);
    }

    // scale up N from 1 to 2 and then from 2 to 3
    for (int i = 0; i < 2; i++) {

      List<PTOperator> unChangedOps = new LinkedList<>(plan.getOperators(o2Meta));
      PTOperator o2p1 = unChangedOps.remove(0);
      Set<PTOperator> expUndeploy = Sets.newHashSet(o2p1);

      PartitioningTest.PartitionLoadWatch.put(o2p1, 1);

      plan.onStatusUpdate(o2p1);
      Assert.assertEquals("repartition event", 1, ctx.events.size());
      ctx.backupRequests = 0;
      ctx.events.remove(0).run();

      Assert.assertEquals("single unifier ", 1, plan.getMergeOperators(o1Meta).size());
      Assert.assertEquals("N partitions after scale up " + o2Meta, 2 + i, plan.getOperators(o2Meta).size());

      for (PTOperator o : plan.getOperators(o2Meta)) {
        Assert.assertNotNull(o.container);
        Assert.assertEquals("number operators ", 1, o.container.getOperators().size());
      }
      Set<PTOperator> expDeploy = Sets.newHashSet(plan.getOperators(o2Meta));
      expDeploy.removeAll(unChangedOps);
      Assert.assertEquals("number of containers", 5 + i, plan.getContainers().size());
      Assert.assertEquals("undeployed operators" + ctx.undeploy, expUndeploy, ctx.undeploy);
      Assert.assertEquals("deployed operators" + ctx.deploy, expDeploy, ctx.deploy);

    }

    // scale down M to 1
    {
      Set<PTOperator> expUndeploy = Sets.newHashSet();
      Set<PTOperator> expDeploy = Sets.newHashSet();
      expUndeploy.addAll(plan.getMergeOperators(o1Meta));
      for (PTOperator o2p : plan.getOperators(o2Meta)) {
        expUndeploy.add(o2p);
        expDeploy.add(o2p);
      }

      for (PTOperator o1p : plan.getOperators(o1Meta)) {
        expUndeploy.add(o1p);
        PartitioningTest.PartitionLoadWatch.put(o1p, -1);
        plan.onStatusUpdate(o1p);
      }

      Assert.assertEquals("repartition event", 1, ctx.events.size());
      ctx.events.remove(0).run();

      Assert.assertEquals("M partitions after scale down " + o1Meta, 1, plan.getOperators(o1Meta).size());
      expUndeploy.removeAll(plan.getOperators(o1Meta));

      Assert.assertEquals("undeploy", expUndeploy, ctx.undeploy);
      Assert.assertEquals("deploy", expDeploy, ctx.deploy);
    }

    // scale up M to 2
    Assert.assertEquals("M partitions " + o1Meta, 1, plan.getOperators(o1Meta).size());
    {
      Set<PTOperator> expUndeploy = Sets.newHashSet();
      Set<PTOperator> expDeploy = Sets.newHashSet();
      for (PTOperator o1p : plan.getOperators(o1Meta)) {
        expUndeploy.add(o1p);
        PartitioningTest.PartitionLoadWatch.put(o1p, 1);
        plan.onStatusUpdate(o1p);
      }

      Assert.assertEquals("repartition event", 1, ctx.events.size());
      ctx.events.remove(0).run();

      Assert.assertEquals("M partitions after scale up " + o1Meta, 2, plan.getOperators(o1Meta).size());
      expDeploy.addAll(plan.getOperators(o1Meta));
      expDeploy.addAll(plan.getMergeOperators(o1Meta));
      for (PTOperator o2p : plan.getOperators(o2Meta)) {
        expUndeploy.add(o2p);
        expDeploy.add(o2p);
        Assert.assertNotNull(o2p.container);
        Assert.assertEquals("number operators ", 1, o2p.container.getOperators().size());
      }
      Assert.assertEquals("undeploy", expUndeploy, ctx.undeploy);
      Assert.assertEquals("deploy", expDeploy, ctx.deploy);
    }

  }

  /**
   * Test covering scenario when only new partitions are added during dynamic partitioning and there
   * are no changes to existing partitions and partition mapping
   */
  @Test
  public void testAugmentedDynamicPartitioning()
  {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.PARTITIONER, new TestAugmentingPartitioner<TestGeneratorInputOperator>(3));
    dag.setOperatorAttribute(o1, OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)new PartitioningTest.PartitionLoadWatch()));
    OperatorMeta o1Meta = dag.getMeta(o1);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    OperatorMeta o2Meta = dag.getMeta(o2);

    dag.addStream("o1.outport1", o1.outport, o2.inport1);

    int maxContainers = 10;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);

    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 4, plan.getContainers().size());

    List<PTOperator> o1ops = plan.getOperators(o1Meta);
    Assert.assertEquals("number of o1 operators", 3, o1ops.size());

    List<PTOperator> o2ops = plan.getOperators(o2Meta);
    Assert.assertEquals("number of o2 operators", 1, o2ops.size());
    Set<PTOperator> expUndeploy = Sets.newLinkedHashSet();
    expUndeploy.addAll(plan.getOperators(o2Meta));
    expUndeploy.add(plan.getOperators(o2Meta).get(0).upstreamMerge.values().iterator().next());

    for (int i = 0; i < 2; ++i) {
      PartitioningTest.PartitionLoadWatch.put(o1ops.get(i), 1);
      plan.onStatusUpdate(o1ops.get(i));
    }

    ctx.backupRequests = 0;
    ctx.events.remove(0).run();

    Assert.assertEquals("number of containers", 6, plan.getContainers().size());

    Assert.assertEquals("undeployed opertors", expUndeploy, ctx.undeploy);
  }

  private class TestAugmentingPartitioner<T> implements Partitioner<T>
  {

    int initalPartitionCount = 1;

    private TestAugmentingPartitioner(int initialPartitionCount)
    {
      this.initalPartitionCount = initialPartitionCount;
    }

    @Override
    public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, PartitioningContext context)
    {
      Collection<Partition<T>> newPartitions = Lists.newArrayList(partitions);
      int numTotal = partitions.size();
      Partition<T> first = partitions.iterator().next();
      if (first.getStats() == null) {
        // Initial partition
        numTotal = initalPartitionCount;
      } else {
        for (Partition<T> p : partitions) {
          // Assumption load is non-negative
          numTotal += p.getLoad();
        }
      }
      T paritionable = first.getPartitionedInstance();
      for (int i = partitions.size(); i < numTotal; ++i) {
        newPartitions.add(new DefaultPartition<>(paritionable));
      }
      return newPartitions;
    }

    @Override
    public void partitioned(Map<Integer, Partition<T>> partitions)
    {

    }
  }

  @Test
  public void testCascadingUnifier()
  {

    LogicalPlan dag = new LogicalPlan();

    PartitioningTestOperator o1 = dag.addOperator("o1", PartitioningTestOperator.class);
    o1.partitionKeys = new Integer[] {0,1,2,3};
    o1.setPartitionCount(o1.partitionKeys.length);

    dag.setOperatorAttribute(o1, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitioningTest.PartitionLoadWatch()}));

    dag.setOutputPortAttribute(o1.outport1, PortContext.UNIFIER_LIMIT, 2);
    OperatorMeta o1Meta = dag.getMeta(o1);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    OperatorMeta o2Meta = dag.getMeta(o2);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 10);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);

    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 9, plan.getContainers().size());

    List<PTOperator> o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1Meta, 4, o1Partitions.size());
    Assert.assertEquals("partitioned map " + o1.partitions, 4, o1.partitions.size());
    List<PTOperator> o2Partitions = plan.getOperators(o2Meta);
    Assert.assertEquals("partitions " + o1Meta, 3, o2Partitions.size());

    for (PTOperator o : o1Partitions) {
      Assert.assertEquals("outputs " + o, 1, o.getOutputs().size());
      for (PTOutput out : o.getOutputs()) {
        Assert.assertEquals("sinks " + out, 1, out.sinks.size());
      }
      Assert.assertNotNull("container " + o, o.getContainer());
    }

    List<PTOperator> o1Unifiers = plan.getMergeOperators(o1Meta);
    Assert.assertEquals("o1Unifiers " + o1Meta, 2, o1Unifiers.size()); // 2 cascadingUnifiers to per-downstream partition unifier(s)
    for (PTOperator o : o1Unifiers) {
      Assert.assertEquals("inputs " + o, 2, o.getInputs().size());
      Assert.assertEquals("outputs " + o, 1, o.getOutputs().size());
      for (PTOutput out : o.getOutputs()) {
        Assert.assertEquals("sinks " + out, 3, out.sinks.size());
        for (PTInput in : out.sinks) {
          // MxN unifier
          Assert.assertTrue(in.target.isUnifier());
          Assert.assertEquals(1, in.target.getOutputs().get(0).sinks.size());
        }
      }
      Assert.assertNotNull("container " + o, o.getContainer());
    }

    for (int i = 0; i < 4; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertTrue(o1Partitions.contains(container.getOperators().get(0)));
    }

    for (int i = 4; i < 6; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertTrue(o1Unifiers.contains(container.getOperators().get(0)));
    }

    for (int i = 6; i < 9; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 2, container.getOperators().size());
      Assert.assertTrue(o2Partitions.contains(container.getOperators().get(0)));
    }

    PTOperator p1 = o1Partitions.get(0);
    StatsListener l = p1.statsListeners.get(0);
    Assert.assertTrue("stats handlers " + p1.statsListeners, l instanceof PartitioningTest.PartitionLoadWatch);
    PartitioningTest.PartitionLoadWatch.put(p1, 1);

    plan.onStatusUpdate(p1);

    Assert.assertEquals("partition scaling triggered", 1, ctx.events.size());

    o1.partitionKeys = new Integer[] {0,1,2,3,4};
    ctx.events.remove(0).run();

    o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1Meta, 5, o1Partitions.size());
    Assert.assertEquals("partitioned map " + o1.partitions, 5, o1.partitions.size());

    o1Unifiers = plan.getMergeOperators(o1Meta);
    Assert.assertEquals("o1Unifiers " + o1Meta, 3, o1Unifiers.size()); // 3(l1)x2(l2)
    for (PTOperator o : o1Unifiers) {
      Assert.assertNotNull("container null: " + o, o.getContainer());
    }

  }

  @Test
  public void testSingleFinalCascadingUnifier()
  {

    LogicalPlan dag = new LogicalPlan();

    PartitioningTestOperator o1 = dag.addOperator("o1", PartitioningTestOperator.class);
    o1.partitionKeys = new Integer[]{0, 1, 2, 3};
    o1.setPartitionCount(3);

    dag.setOperatorAttribute(o1, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitioningTest.PartitionLoadWatch()}));
    dag.setOutputPortAttribute(o1.outport1, PortContext.UNIFIER_LIMIT, 2);
    dag.setOutputPortAttribute(o1.outport1, PortContext.UNIFIER_SINGLE_FINAL, true);
    OperatorMeta o1Meta = dag.getMeta(o1);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    OperatorMeta o2Meta = dag.getMeta(o2);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 12);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);

    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 10, plan.getContainers().size());

    List<PTOperator> o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1Meta, 4, o1Partitions.size());
    Assert.assertEquals("partitioned map " + o1.partitions, 4, o1.partitions.size());
    List<PTOperator> o2Partitions = plan.getOperators(o2Meta);
    Assert.assertEquals("partitions " + o1Meta, 3, o2Partitions.size());

    for (PTOperator o : o1Partitions) {
      Assert.assertEquals("outputs " + o, 1, o.getOutputs().size());
      for (PTOutput out : o.getOutputs()) {
        Assert.assertEquals("sinks " + out, 1, out.sinks.size());
      }
      Assert.assertNotNull("container " + o, o.getContainer());
    }

    List<PTOperator> o1Unifiers = plan.getMergeOperators(o1Meta);
    Assert.assertEquals("o1Unifiers " + o1Meta, 3, o1Unifiers.size()); // 2 cascadingUnifiers and one-downstream partition unifier
    List<PTOperator> finalUnifiers = new ArrayList<>();
    for (PTOperator o : o1Unifiers) {
      Assert.assertEquals("inputs " + o, 2, o.getInputs().size());
      Assert.assertEquals("outputs " + o, 1, o.getOutputs().size());
      List<PTInput> sinks = o.getOutputs().get(0).sinks;
      boolean finalUnifier = sinks.size() > 0 ? (sinks.get(0).target.getOperatorMeta() == o2Meta) : false;
      if (!finalUnifier) {
        for (PTOutput out : o.getOutputs()) {
          Assert.assertEquals("sinks " + out, 1, out.sinks.size());
          Assert.assertTrue(out.sinks.get(0).target.isUnifier());
        }
      } else {
        for (PTOutput out : o.getOutputs()) {
          Assert.assertEquals("sinks " + out, 3, out.sinks.size());
          for (PTInput in : out.sinks) {
            Assert.assertFalse(in.target.isUnifier());
          }
        }
        finalUnifiers.add(o);
      }
      Assert.assertNotNull("container " + o, o.getContainer());
    }
    Assert.assertEquals("o1 final unifiers", 1, finalUnifiers.size());

    for (int i = 0; i < 4; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertTrue(o1Partitions.contains(container.getOperators().get(0)));
    }

    for (int i = 4; i < 7; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertTrue(o1Unifiers.contains(container.getOperators().get(0)));
    }

    for (int i = 7; i < 10; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertTrue(o2Partitions.contains(container.getOperators().get(0)));
    }

    PTOperator p1 = o1Partitions.get(0);
    StatsListener l = p1.statsListeners.get(0);
    Assert.assertTrue("stats handlers " + p1.statsListeners, l instanceof PartitioningTest.PartitionLoadWatch);
    PartitioningTest.PartitionLoadWatch.put(p1, 1);

    plan.onStatusUpdate(p1);

    Assert.assertEquals("partition scaling triggered", 1, ctx.events.size());

    o1.partitionKeys = new Integer[] {0,1,2,3,4};
    ctx.events.remove(0).run();

    o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1Meta, 5, o1Partitions.size());
    Assert.assertEquals("partitioned map " + o1.partitions, 5, o1.partitions.size());

    o1Unifiers = plan.getMergeOperators(o1Meta);
    Assert.assertEquals("o1Unifiers " + o1Meta, 4, o1Unifiers.size()); // 3(l1)x2(l2)
    for (PTOperator o : o1Unifiers) {
      Assert.assertNotNull("container null: " + o, o.getContainer());
    }

  }

  @Test
  public void testSingleFinalUnifierInputOverride()
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    OperatorMeta o1Meta = dag.getMeta(o1);

    GenericTestOperator o2 =  dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    dag.setInputPortAttribute(o2.inport1, PortContext.UNIFIER_SINGLE_FINAL, true);
    OperatorMeta o2Meta = dag.getMeta(o2);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 10);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);

    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 6, plan.getContainers().size());

    Assert.assertEquals("o1 merge unifiers", 1, plan.getMergeOperators(o1Meta).size());

    dag.setOutputPortAttribute(o1.outport1, PortContext.UNIFIER_SINGLE_FINAL, false);
    ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 6, plan.getContainers().size());

    Assert.assertEquals("o1 merge unifiers", 1, plan.getMergeOperators(o1Meta).size());

    dag.setOutputPortAttribute(o1.outport1, PortContext.UNIFIER_SINGLE_FINAL, true);
    dag.setInputPortAttribute(o2.inport1, PortContext.UNIFIER_SINGLE_FINAL, false);
    ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);
    plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 5, plan.getContainers().size());

    Set<String> expectedNames = Sets.newHashSet(o1Meta.getMeta(o1.outport1).getUnifierMeta().getName(), o2Meta.getName());
    for (int i = 3; i < 5; ++i) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("o2 container size", 2, container.getOperators().size());

      Set<String> names = Sets.newHashSet();
      for (PTOperator operator : container.getOperators()) {
        names.add(operator.getOperatorMeta().getName());
      }
      Assert.assertEquals("o2 container operators", expectedNames, names);
    }
  }

  @Test
  public void testSingleFinalUnifierMultiInput()
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    OperatorMeta o1Meta = dag.getMeta(o1);

    GenericTestOperator o2 =  dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(4));
    dag.setInputPortAttribute(o2.inport1, PortContext.UNIFIER_SINGLE_FINAL, true);
    OperatorMeta o2Meta = dag.getMeta(o2);

    GenericTestOperator o3 =  dag.addOperator("o3", GenericTestOperator.class);
    dag.setOperatorAttribute(o3, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    OperatorMeta o3Meta = dag.getMeta(o3);

    dag.addStream("o1o2o3", o1.outport1, o2.inport1, o3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 12);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, ctx);

    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 10, plan.getContainers().size());

    Assert.assertEquals("o1 merge unifiers", 1, plan.getMergeOperators(o1Meta).size());

    // Check the merge unifier
    {
      PTContainer container = plan.getContainers().get(3);
      Assert.assertEquals("number of operators " + container, 1, container.getOperators().size());
      PTOperator operator = container.getOperators().get(0);
      Assert.assertTrue("unifier check " + operator, operator.isUnifier());
      Assert.assertEquals("operator meta " + operator, o1Meta.getMeta(o1.outport1).getUnifierMeta(), operator.getOperatorMeta());
    }

    int numberO2Containers = 0;
    int numberO3Containers = 0;
    Set<String> expectedNames = Sets.newHashSet(o1Meta.getMeta(o1.outport1).getUnifierMeta().getName(), o3Meta.getName());
    for (int i = 4; i < 10; i++) {
      PTContainer container = plan.getContainers().get(i);
      List<PTOperator> operators = container.getOperators();
      Assert.assertTrue("expected operator count " + container, (operators.size() <= 2) && (operators.size() > 0));
      if (operators.size() == 1) {
        Assert.assertEquals("operator in container " + container, o2Meta, operators.get(0).getOperatorMeta());
        ++numberO2Containers;
      } else if (operators.size() == 2) {
        Set<String> names = Sets.newHashSet();
        for (PTOperator operator : container.getOperators()) {
          names.add(operator.getOperatorMeta().getName());
        }
        Assert.assertEquals("container operators " + container, expectedNames, names);
        ++numberO3Containers;
      }
    }
    Assert.assertEquals("number o2 containers", 4, numberO2Containers);
    Assert.assertEquals("number o3 containers", 2, numberO3Containers);
  }

  @Test
  public void testContainerSize()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    dag.setOperatorAttribute(o1,OperatorContext.VCORES,1);
    dag.setOperatorAttribute(o2,OperatorContext.VCORES,2);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1);

    dag.setOperatorAttribute(o2, OperatorContext.MEMORY_MB, 4000);
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 2);

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());
    Assert.assertEquals("memory container 1", 2560, plan.getContainers().get(0).getRequiredMemoryMB());
    Assert.assertEquals("vcores container 1", 1, plan.getContainers().get(0).getRequiredVCores());
    Assert.assertEquals("memory container 2", 4512, plan.getContainers().get(1).getRequiredMemoryMB());
    Assert.assertEquals("vcores container 2", 2, plan.getContainers().get(1).getRequiredVCores());
    Assert.assertEquals("number of operators in container 1", 2, plan.getContainers().get(0).getOperators().size());
  }

  @Test
  public void testContainerCores()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    GenericTestOperator o4 = dag.addOperator("o4", GenericTestOperator.class);
    GenericTestOperator o5 = dag.addOperator("o5", GenericTestOperator.class);
    GenericTestOperator o6 = dag.addOperator("o6", GenericTestOperator.class);
    dag.setOperatorAttribute(o1,OperatorContext.VCORES,1);
    dag.setOperatorAttribute(o2,OperatorContext.VCORES,2);
    dag.setOperatorAttribute(o3,OperatorContext.VCORES,3);
    dag.setOperatorAttribute(o4,OperatorContext.VCORES,4);
    dag.setOperatorAttribute(o5,OperatorContext.VCORES,5);
    dag.setOperatorAttribute(o6,OperatorContext.VCORES,6);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1, o4.inport1).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("o3.output1", o3.outport1, o5.inport1).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("o4.output1", o4.outport1, o5.inport2).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("o5.output1", o5.outport1, o6.inport1).setLocality(Locality.CONTAINER_LOCAL);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 2);

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());

    Assert.assertEquals("number of containers", 1, plan.getContainers().size());
    Assert.assertEquals("vcores container 1 is 12", 12, plan.getContainers().get(0).getRequiredVCores());
  }

  @Test
  public void testContainerSizeWithPartitioning()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 10);
    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", 5, plan.getContainers().size());
    PTContainer container;
    for (int i = 0; i < 5; i++) {
      container = plan.getContainers().get(i);
      if (container.getOperators().size() == 1) {
        Assert.assertEquals("container memory is 1536 for container :" + container, 1536, container.getRequiredMemoryMB());
      }
      if (container.getOperators().size() == 2) {
        Assert.assertEquals("container memory is 2048 for container :" + container, 2048, container.getRequiredMemoryMB());
      }
    }
  }

  private class TestPartitioner<T extends Operator> extends StatelessPartitioner<T>
  {
    private static final long serialVersionUID = 1L;
    final List<Partition<T>> extraPartitions = Lists.newArrayList();

    @Override
    public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, PartitioningContext context)
    {
      if (!extraPartitions.isEmpty()) {
        partitions.addAll(extraPartitions);
        return partitions;
      }

      Collection<Partition<T>> newPartitions = super.definePartitions(partitions, context);
      if (context.getParallelPartitionCount() > 0 && newPartitions.size() < context.getParallelPartitionCount()) {
        // parallel partitioned, fill to requested count
        for (int i = newPartitions.size(); i < context.getParallelPartitionCount(); i++) {
          newPartitions.add(new DefaultPartition<>(partitions.iterator().next().getPartitionedInstance()));
        }
      }
      return newPartitions;
    }
  }

  @Test
  public void testDefaultPartitionerWithParallel() throws InterruptedException
  {
    final MutableInt loadInd = new MutableInt();

    StatsListener listener = new StatsListener()
    {
      @Override
      public Response processStats(BatchedOperatorStats stats)
      {
        Response response = new Response();
        response.repartitionRequired = true;
        response.loadIndicator = loadInd.intValue();
        return response;
      }
    };

    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator nodeX = dag.addOperator("X", GenericTestOperator.class);
    dag.setOperatorAttribute(nodeX, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    dag.setOperatorAttribute(nodeX, Context.OperatorContext.STATS_LISTENERS, Lists.newArrayList(listener));

    GenericTestOperator nodeY = dag.addOperator("Y", GenericTestOperator.class);
    dag.setOperatorAttribute(nodeY, Context.OperatorContext.PARTITIONER, new TestPartitioner<GenericTestOperator>());

    GenericTestOperator nodeZ = dag.addOperator("Z", GenericTestOperator.class);

    dag.addStream("Stream1", nodeX.outport1, nodeY.inport1, nodeZ.inport1);
    dag.addStream("Stream2", nodeX.outport2, nodeY.inport2, nodeZ.inport2);

    dag.setInputPortAttribute(nodeY.inport1, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(nodeY.inport2, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(nodeZ.inport1, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(nodeZ.inport2, Context.PortContext.PARTITION_PARALLEL, true);

    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    LogicalPlan.OperatorMeta metaOfX = dag.getMeta(nodeX);
    LogicalPlan.OperatorMeta metaOfY = dag.getMeta(nodeY);

    Assert.assertEquals("number operators " + metaOfX.getName(), 2, plan.getOperators(metaOfX).size());
    Assert.assertEquals("number operators " + metaOfY.getName(), 2, plan.getOperators(metaOfY).size());

    List<PTOperator> ptOfX = plan.getOperators(metaOfX);

    for (PTOperator physicalX : ptOfX) {
      Assert.assertEquals("2 streams " + physicalX.getOutputs(), 2, physicalX.getOutputs().size());
      for (PTOutput outputPort : physicalX.getOutputs()) {
        Set<PTOperator> dopers = Sets.newHashSet();
        Assert.assertEquals("sink of " + metaOfX.getName() + " id " + physicalX.id + " port " + outputPort.portName,
            2, outputPort.sinks.size());
        for (PTInput inputPort : outputPort.sinks) {
          dopers.add(inputPort.target);
        }
        Assert.assertEquals(2, dopers.size());
      }
    }

    //Invoke redo-partition of PhysicalPlan, no partition change
    loadInd.setValue(0);
    for (PTOperator ptOperator : ptOfX) {
      plan.onStatusUpdate(ptOperator);
    }
    ctx.events.remove(0).run();

    for (PTOperator physicalX : ptOfX) {
      Assert.assertEquals("2 streams " + physicalX.getOutputs(), 2, physicalX.getOutputs().size());
      for (PTOutput outputPort : physicalX.getOutputs()) {
        Set<PTOperator> dopers = Sets.newHashSet();
        Assert.assertEquals("sink of " + metaOfX.getName() + " id " + physicalX.id + " port " + outputPort.portName,
            2, outputPort.sinks.size());
        for (PTInput inputPort : outputPort.sinks) {
          dopers.add(inputPort.target);
        }
        Assert.assertEquals(2, dopers.size());
      }
    }

    //scale up by splitting first partition
    loadInd.setValue(1);
    plan.onStatusUpdate(ptOfX.get(0));
    ctx.events.get(0).run();

    List<PTOperator> ptOfXScaleUp = plan.getOperators(metaOfX);
    Assert.assertEquals("3 partitons " + ptOfXScaleUp, 3, ptOfXScaleUp.size());
    for (PTOperator physicalX : ptOfXScaleUp) {
      Assert.assertEquals("2 streams " + physicalX.getOutputs(), 2, physicalX.getOutputs().size());
      for (PTOutput outputPort : physicalX.getOutputs()) {
        Set<PTOperator> dopers = Sets.newHashSet();
        Assert.assertEquals("sink of " + metaOfX.getName() + " id " + physicalX.id + " port " + outputPort.portName,
            2, outputPort.sinks.size());
        for (PTInput inputPort : outputPort.sinks) {
          dopers.add(inputPort.target);
        }
        Assert.assertEquals(2, dopers.size());
      }
    }

  }

  @Test
  public void testContainersForSlidingWindow()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    dag.setOperatorAttribute(o1, OperatorContext.SLIDE_BY_WINDOW_COUNT, 2);
    dag.getOperatorMeta("o1").getMeta(o1.outport1).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB, 2000);
    dag.getOperatorMeta("o1").getMeta(o1.outport2).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB, 4000);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    dag.addStream("o1.outport2", o1.outport2, o2.inport2);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1);
    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", 5, plan.getContainers().size());
    boolean sawOutput1Slider = false;
    boolean sawOutput2Slider = false;
    for (PTContainer container : plan.getContainers()) {
      Assert.assertEquals("number of operators in each container is 1", container.operators.size(), 1);
      if (container.operators.get(0).isUnifier()) {
        String name = container.operators.get(0).getName();
        if (name.equals("o1.outport1#slider")) {
          sawOutput1Slider = true;
          Assert.assertEquals("container memory is 2512", container.getRequiredMemoryMB(), 2512);
        } else if (name.equals("o1.outport2#slider")) {
          sawOutput2Slider = true;
          Assert.assertEquals("container memory is 2512", container.getRequiredMemoryMB(), 4512);
        }
      }
    }
    Assert.assertEquals("Found output1 slider", true, sawOutput1Slider);
    Assert.assertEquals("Found output2 slider", true, sawOutput2Slider);
  }

  @Test
  public void testMxNPartitionForSlidingWindow()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    dag.setOperatorAttribute(o1, OperatorContext.SLIDE_BY_WINDOW_COUNT, 2);
    dag.setOperatorAttribute(o1, OperatorContext.PARTITIONER, new StatelessPartitioner<>(2));
    dag.getOperatorMeta("o1").getMeta(o1.outport1).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB, 1024);
    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<>(2));
    dag.setOperatorAttribute(o2, OperatorContext.SLIDE_BY_WINDOW_COUNT, 2);
    dag.setOperatorAttribute(o2, OperatorContext.APPLICATION_WINDOW_COUNT, 4);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1);
    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", 9, plan.getContainers().size());
  }

  @Test
  public void testParallelPartitionForSlidingWindow()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.SLIDE_BY_WINDOW_COUNT, 2);
    dag.setOperatorAttribute(o1, OperatorContext.PARTITIONER, new StatelessPartitioner<>(2));
    dag.setInputPortAttribute(o2.inport1, PortContext.PARTITION_PARALLEL, true);
    dag.setOperatorAttribute(o1, OperatorContext.APPLICATION_WINDOW_COUNT, 4);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1);
    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", 7, plan.getContainers().size());
  }

  static class StatsListenerOperatorOld extends BaseOperator implements StatsListener
  {
    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      return null;
    }
  }

  static class StatsListenerOperator extends GenericNodeTest.GenericOperator implements StatsListenerWithContext
  {
    @Override
    public Response processStats(BatchedOperatorStats stats, StatsListenerContext context)
    {
      return null;
    }

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      return null;
    }
  }

  /**
   * Test that internally all stats listeners are handled through StatsListenerWithContext.
   * Following cases are tested
   *
   * Operator implementing StatsListener
   * Operator implementing StatsListenerWithContext
   * Operator with STATS_LISTENERS attribute set to StatsListener
   * Operator with STATS_LISTENERS attribute set to StatsListenerWithContext
   */
  @Test
  public void testStatsListenerContextWrappers()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    StatsListenerOperator o1 = dag.addOperator("o1", new StatsListenerOperator());
    GenericTestOperator o2 = dag.addOperator("o2", new GenericTestOperator());
    dag.setAttribute(o2, OperatorContext.STATS_LISTENERS,
        Lists.<StatsListener>newArrayList(mock(StatsListener.class)));

    GenericTestOperator o3 = dag.addOperator("o3", new GenericTestOperator());
    dag.setAttribute(o3, OperatorContext.STATS_LISTENERS,
        Lists.<StatsListener>newArrayList(mock(StatsListenerWithContext.class)));

    StatsListenerOperatorOld o4 = dag.addOperator("o4", new StatsListenerOperatorOld());

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());

    PTOperator p1 = plan.getOperators(dag.getMeta(o1)).get(0);
    StatsListener l = p1.statsListeners.get(0);
    Assert.assertTrue("Operator stats listener is wrapped ", l instanceof StatsListenerWithContext);

    PTOperator p2 = plan.getOperators(dag.getMeta(o2)).get(0);
    l = p1.statsListeners.get(0);
    Assert.assertTrue("Operator stats listener is wrapped ", l instanceof StatsListenerWithContext);

    PTOperator p3 = plan.getOperators(dag.getMeta(o3)).get(0);
    l = p1.statsListeners.get(0);
    Assert.assertTrue("Operator stats listener is wrapped ", l instanceof StatsListenerWithContext);

    PTOperator p4 = plan.getOperators(dag.getMeta(o4)).get(0);
    l = p1.statsListeners.get(0);
    Assert.assertTrue("Operator stats listener is wrapped ", l instanceof StatsListenerWithContext);
  }
}

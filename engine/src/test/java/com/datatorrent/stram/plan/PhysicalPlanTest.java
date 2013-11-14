/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.PartitionableOperator;
import com.datatorrent.api.PartitionableOperator.Partition;
import com.datatorrent.api.PartitionableOperator.PartitionKeys;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.stram.PartitioningTest;
import com.datatorrent.stram.PartitioningTest.TestInputOperator;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.physical.OperatorPartitions;
import com.datatorrent.stram.plan.physical.OperatorPartitions.PartitionImpl;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PTOperator.PTInput;
import com.datatorrent.stram.plan.physical.PTOperator.PTOutput;
import com.datatorrent.stram.plan.physical.PhysicalPlan;

public class PhysicalPlanTest {
  public static class PartitioningTestStreamCodec extends DefaultStatefulStreamCodec<Object> {
    @Override
    public int getPartition(Object o) {
      return PartitioningTestOperator.PARTITION_KEYS[ o.hashCode() % PartitioningTestOperator.PARTITION_KEYS.length];
    }

  }

  public static class PartitioningTestOperator extends GenericTestOperator implements PartitionableOperator {
    final public static Integer[] PARTITION_KEYS = {0, 1, 2};
    final static String INPORT_WITH_CODEC = "inportWithCodec";
    public Integer[] partitionKeys = {0, 1, 2};

    @InputPortFieldAnnotation(name = INPORT_WITH_CODEC, optional = true)
    final public transient InputPort<Object> inportWithCodec = new DefaultInputPort<Object>() {
      @Override
      public Class<? extends StreamCodec<Object>> getStreamCodec() {
        return PartitioningTestStreamCodec.class;
      }

      @Override
      final public void process(Object payload) {
      }

    };

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacityIgnored) {
      List<Partition<?>> newPartitions = new ArrayList<Partition<?>>(this.partitionKeys.length);
      Partition<PartitioningTestOperator> templatePartition = (Partition<PartitioningTestOperator>)partitions.iterator().next();
      for (int i = 0; i < partitionKeys.length; i++) {
        Partition<PartitioningTestOperator> p = templatePartition.getInstance(new PartitioningTestOperator());
        p.getPartitionKeys().put(this.inport1, new PartitionKeys(2, Sets.newHashSet(partitionKeys[i])));
        p.getPartitionKeys().put(this.inportWithCodec, new PartitionKeys(2, Sets.newHashSet(partitionKeys[i])));
        newPartitions.add(p);
      }
      return newPartitions;
    }

  }

  @Test
  public void testStaticPartitioning() {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    PartitioningTestOperator node2 = dag.addOperator("node2", PartitioningTestOperator.class);
    GenericTestOperator mergeNode = dag.addOperator("mergeNode", GenericTestOperator.class);

    dag.addStream("n1.outport1", node1.outport1, node2.inport1, node2.inportWithCodec);
    dag.addStream("mergeStream", node2.outport1, mergeNode.inport1);

    dag.getMeta(node2).getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, 3);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);

    OperatorMeta node2Decl = dag.getOperatorMeta(node2.getName());

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());

    List<PTOperator> n2Instances = plan.getOperators(node2Decl);
    Assert.assertEquals("partition instances " + n2Instances, PartitioningTestOperator.PARTITION_KEYS.length, n2Instances.size());
    for (int i = 0; i < PartitioningTestOperator.PARTITION_KEYS.length; i++) {
      PTOperator po = n2Instances.get(i);
      Map<String, PTInput> inputsMap = new HashMap<String, PTInput>();
      for (PTInput input: po.getInputs()) {
        inputsMap.put(input.portName, input);
        Assert.assertEquals("partitions " + input, Sets.newHashSet(PartitioningTestOperator.PARTITION_KEYS[i]), input.partitions.partitions);
        Assert.assertEquals("codec " + input.logicalStream, PartitioningTestStreamCodec.class, input.logicalStream.getCodecClass());
      }
      Assert.assertEquals("number inputs " + inputsMap, Sets.newHashSet(PartitioningTestOperator.IPORT1, PartitioningTestOperator.INPORT_WITH_CODEC), inputsMap.keySet());
    }

    Collection<PTOperator> unifiers = plan.getMergeOperators(dag.getMeta(node2));
    Assert.assertEquals("number unifiers", 1, unifiers.size());
    Assert.assertNotNull("unifier container " + unifiers, unifiers.iterator().next().getContainer());
  }

  @Test
  public void testDefaultPartitioning() {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.addStream("node1.outport1", node1.outport1, node2.inport2, node2.inport1);

    int initialPartitionCount = 5;
    OperatorMeta node2Decl = dag.getOperatorMeta(node2.getName());
    node2Decl.getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, initialPartitionCount);

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());

    List<PTOperator> n2Instances = plan.getOperators(node2Decl);
    Assert.assertEquals("partition instances " + n2Instances, initialPartitionCount, n2Instances.size());

    List<Integer> assignedPartitionKeys = Lists.newArrayList();

    for (int i = 0; i < n2Instances.size(); i++) {
      PTOperator partitionInstance = n2Instances.get(i);
      Partition<?> p = partitionInstance.getPartition();
      Assert.assertNotNull("partition null: " + partitionInstance, p);
      Map<InputPort<?>, PartitionKeys> pkeys = p.getPartitionKeys();
      Assert.assertNotNull("partition keys null: " + partitionInstance, pkeys);
      Assert.assertEquals("partition keys size: " + pkeys, 1, pkeys.size()); // one port partitioned
      // default partitioning does not clone the operator
      Assert.assertEquals("partition operator: " + pkeys, node2, partitionInstance.getPartition().getOperator());
      InputPort<?> expectedPort = node2.inport2;
      Assert.assertEquals("partition port: " + pkeys, expectedPort, pkeys.keySet().iterator().next());

      Assert.assertEquals("partition mask: " + pkeys, "111", Integer.toBinaryString(pkeys.get(expectedPort).mask));
      Set<Integer> pks = pkeys.get(expectedPort).partitions;
      Assert.assertTrue("number partition keys: " + pkeys, pks.size() == 1 || pks.size() == 2);
      assignedPartitionKeys.addAll(pks);
    }

    int expectedMask = Integer.parseInt("111", 2);
    Assert.assertEquals("assigned partitions ", expectedMask+1,  assignedPartitionKeys.size());
    for (int i=0; i<=expectedMask; i++) {
      Assert.assertTrue(""+assignedPartitionKeys, assignedPartitionKeys.contains(i));
    }

  }

  @Test
  public void testRepartitioningScaleUp() {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator mergeNode = dag.addOperator("mergeNode", GenericTestOperator.class);

    dag.addStream("n1.outport1", node1.outport1, node2.inport1, node2.inport2);
    dag.addStream("mergeStream", node2.outport1, mergeNode.inport1);

    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);

    OperatorMeta node2Meta = dag.getOperatorMeta(node2.getName());
    node2Meta.getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, 2);
    node2Meta.getAttributes().put(OperatorContext.PARTITION_TPS_MIN, 0);
    node2Meta.getAttributes().put(OperatorContext.PARTITION_TPS_MAX, 5);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());

    List<PTOperator> n2Instances = plan.getOperators(node2Meta);
    Assert.assertEquals("partition instances " + n2Instances, 2, n2Instances.size());
    PTOperator po = n2Instances.get(0);
    PTOperator po2 = n2Instances.get(1);

    Set<PTOperator> expUndeploy = Sets.newHashSet(plan.getOperators(dag.getMeta(mergeNode)));
    expUndeploy.add(po);
    expUndeploy.addAll(plan.getMergeOperators(node2Meta));

    // verify load update generates expected events per configuration
    Assert.assertEquals("stats handlers " + po, 1, po.statsMonitors.size());
    PhysicalPlan.StatsHandler sm = po.statsMonitors.get(0);
    Assert.assertTrue("stats handlers " + po.statsMonitors, sm instanceof PhysicalPlan.PartitionLoadWatch);
    sm.onThroughputUpdate(po, 0);
    Assert.assertEquals("load event triggered", 0, ctx.events.size());
    sm.onThroughputUpdate(po, 3);
    Assert.assertEquals("load within range", 0, ctx.events.size());
    sm.onThroughputUpdate(po, 10);
    Assert.assertEquals("load exceeds max", 1, ctx.events.size());

    ctx.events.remove(0).run();

    Assert.assertEquals("new partitions", 3, plan.getOperators(node2Meta).size());
    Assert.assertTrue("", plan.getOperators(node2Meta).contains(po2));

    for (PTOperator partition : plan.getOperators(node2Meta)) {
      Assert.assertNotNull("container null " + partition, partition.getContainer());
      Assert.assertEquals("outputs " + partition, 1, partition.getOutputs().size());
      Assert.assertEquals("downstream operators " + partition.getOutputs().get(0).sinks, 1, partition.getOutputs().get(0).sinks.size());
    }
    Assert.assertEquals("" + ctx.undeploy, expUndeploy, ctx.undeploy);

    Set<PTOperator> expDeploy = Sets.newHashSet(plan.getOperators(dag.getMeta(mergeNode)));
    expDeploy.addAll(plan.getOperators(node2Meta));
    expDeploy.remove(po2);
    expDeploy.addAll(plan.getMergeOperators(node2Meta));

    Assert.assertEquals("" + ctx.deploy, expDeploy, ctx.deploy);
    Assert.assertEquals("Count of storage requests", 0, ctx.backupRequests);
  }

  /**
   * Test partitioning of an input operator (no input port).
   * Cover aspects that are not part of generic operator test.
   */
  @Test
  public void testInputOperatorPartitioning() {
    LogicalPlan dag = new LogicalPlan();
    TestInputOperator<Object> o1 = dag.addOperator("o1", new TestInputOperator<Object>());
    OperatorMeta o1Meta = dag.getOperatorMeta(o1.getName());
    o1Meta.getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, 2);
    o1Meta.getAttributes().put(OperatorContext.PARTITION_TPS_MIN, 0);
    o1Meta.getAttributes().put(OperatorContext.PARTITION_TPS_MAX, 5);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 2, plan.getContainers().size());

    List<PTOperator> o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partition instances " + o1Partitions, 2, o1Partitions.size());
    PTOperator o1p1 = o1Partitions.get(0);

    // verify load update generates expected events per configuration
    Assert.assertEquals("stats handlers " + o1p1, 1, o1p1.statsMonitors.size());
    PhysicalPlan.StatsHandler sm = o1p1.statsMonitors.get(0);
    Assert.assertTrue("stats handlers " + o1p1.statsMonitors, sm instanceof PhysicalPlan.PartitionLoadWatch);
    sm.onThroughputUpdate(o1p1, 0);
    Assert.assertEquals("load event triggered", 0, ctx.events.size());
    sm.onThroughputUpdate(o1p1, 3);
    Assert.assertEquals("load within range", 0, ctx.events.size());
    sm.onThroughputUpdate(o1p1, 10);
    Assert.assertEquals("load exceeds max", 1, ctx.events.size());

    Runnable r = ctx.events.remove(0);
    r.run();
    ((PhysicalPlan.PartitionLoadWatch)sm).evalIntervalMillis = -1; // no delay
    Assert.assertEquals("operators after scale up", 3, plan.getOperators(o1Meta).size());
    for (PTOperator p : plan.getOperators(o1Meta)) {
      Assert.assertTrue(p.checkpointWindows.isEmpty());
      sm.onThroughputUpdate(p, -1);
    }
    ctx.events.remove(0).run();
    Assert.assertEquals("operators after scale down", 2, plan.getOperators(o1Meta).size());
/*
    // ensure scale up maintains min checkpoint
    long checkpoint=1;
    for (PTOperator p : plan.getOperators(o1Meta)) {
      p.checkpointWindows.add(checkpoint);
      p.setRecoveryCheckpoint(checkpoint);
      sm.onThroughputUpdate(p, 10);
    }
    ctx.events.remove(0).run();
    Assert.assertEquals("operators after scale up (2)", 4, plan.getOperators(o1Meta).size());
    for (PTOperator p : plan.getOperators(o1Meta)) {
      Assert.assertEquals("checkpoints " + p.checkpointWindows, p.checkpointWindows.size(), 1);
    }
*/
  }

  @Test
  public void testRepartitioningScaleDown() {
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

    OperatorMeta node2Meta = dag.getOperatorMeta(o2.getName());
    node2Meta.getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, 8);
    node2Meta.getAttributes().put(OperatorContext.PARTITION_TPS_MIN, 3);
    node2Meta.getAttributes().put(OperatorContext.PARTITION_TPS_MAX, 5);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());

    List<PTOperator> n2Instances = plan.getOperators(node2Meta);
    Assert.assertEquals("partition instances " + n2Instances, 8, n2Instances.size());
    PTOperator po = n2Instances.get(0);

    Collection<PTOperator> unifiers = plan.getMergeOperators(node2Meta);
    Assert.assertEquals("unifiers " + node2Meta, 0, unifiers.size());

    Collection<PTOperator> o3unifiers = plan.getMergeOperators(o3Meta);
    Assert.assertEquals("unifiers " + o3Meta, 1, o3unifiers.size());
    PTOperator o3unifier = o3unifiers.iterator().next();
    Assert.assertEquals("unifier inputs " + o3unifier, 8, o3unifier.getInputs().size());

    Set<PTOperator> expUndeploy = Sets.newHashSet(plan.getOperators(dag.getMeta(mergeNode)));
    expUndeploy.addAll(n2Instances);
    expUndeploy.addAll(plan.getOperators(o3Meta));
    expUndeploy.addAll(plan.getMergeOperators(o3Meta));

    // verify load update generates expected events per configuration
    Assert.assertEquals("stats handlers " + po, 1, po.statsMonitors.size());
    PhysicalPlan.StatsHandler sm = po.statsMonitors.get(0);
    ((PhysicalPlan.PartitionLoadWatch)sm).evalIntervalMillis = -1; // no delay

    Assert.assertTrue("stats handlers " + po.statsMonitors, sm instanceof PhysicalPlan.PartitionLoadWatch);
    sm.onThroughputUpdate(po, 5);
    Assert.assertEquals("load upper bound", 0, ctx.events.size());
    sm.onThroughputUpdate(po, 3);
    Assert.assertEquals("load lower bound", 0, ctx.events.size());
    sm.onThroughputUpdate(po, 2);
    Assert.assertEquals("load below min", 1, ctx.events.size());

    Runnable r = ctx.events.remove(0);
    r.run();

    // expect operators unchanged
    Assert.assertEquals("partitions unchanged", Sets.newHashSet(n2Instances), Sets.newHashSet(plan.getOperators(node2Meta)));

    for (PTOperator o : n2Instances) {
      sm.onThroughputUpdate(o, 2);
    }
    Assert.assertEquals("load below min", 1, ctx.events.size());
    ctx.events.remove(0).run();
    Assert.assertEquals("partitions merged", 4, plan.getOperators(node2Meta).size());
    Assert.assertEquals("unifier inputs after scale down " + o3unifier, 4, o3unifier.getInputs().size());

    for (PTOperator p : plan.getOperators(o3Meta)) {
      Assert.assertEquals("outputs " + p.getOutputs(), 1, p.getOutputs().size());
    }

    for (PTOperator p : plan.getOperators(node2Meta)) {
      PartitionKeys pks = p.getPartition().getPartitionKeys().values().iterator().next();
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

    Set<PTOperator> expDeploy = Sets.newHashSet(plan.getOperators(dag.getMeta(mergeNode)));
    expDeploy.addAll(plan.getOperators(node2Meta));
    expDeploy.addAll(plan.getOperators(o3Meta));
    expDeploy.addAll(plan.getMergeOperators(o3Meta));

    Assert.assertEquals("" + ctx.deploy, expDeploy, ctx.deploy);
    for (PTOperator oper : ctx.deploy) {
      Assert.assertNotNull("container " + oper , oper.getContainer());
    }
    Assert.assertEquals("Count of storage requests", 0, ctx.backupRequests);
  }

  /**
   * Test unifier gets removed when number partitions drops to 1.
   */
  @Test
  public void testRepartitioningScaleDownSinglePartition() {
    LogicalPlan dag = new LogicalPlan();

    TestInputOperator<?> o1 = dag.addOperator("o1", TestInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.output, o2.inport1);
    OperatorMeta o1Meta = dag.getMeta(o1);
    dag.setAttribute(o1, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    dag.setAttribute(o1, OperatorContext.PARTITION_STATS_HANDLER, PartitioningTest.PartitionLoadWatch.class.getName());

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    List<PTOperator> o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1Partitions, 2, o1Partitions.size());
    PTOperator o1p1 = o1Partitions.get(0);
    PTOperator p1Doper = o1p1.getOutputs().get(0).sinks.get(0).target;
    Assert.assertTrue("", p1Doper.getOperatorMeta() == o1Meta);
    Assert.assertNotNull("unifier ", p1Doper.getUnifier());

    Collection<PTOperator> o1Unifiers = plan.getMergeOperators(o1Meta);
    Assert.assertEquals("unifiers " + o1Meta, 1, o1Unifiers.size());

    PhysicalPlan.StatsHandler sm = o1p1.statsMonitors.get(0);
    Assert.assertTrue("stats handlers " + o1p1.statsMonitors, sm instanceof PartitioningTest.PartitionLoadWatch);
    PartitioningTest.PartitionLoadWatch.loadIndicators.put(o1p1, -1);
    PartitioningTest.PartitionLoadWatch.loadIndicators.put(o1Partitions.get(1), -1);
    sm.onThroughputUpdate(o1p1, -1);
    sm.onThroughputUpdate(o1Partitions.get(1), -1);
    Assert.assertEquals("partition scaling triggered", 1, ctx.events.size());
    ctx.events.remove(0).run();

    List<PTOperator> o1NewPartitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1NewPartitions, 1, o1NewPartitions.size());

    Collection<PTOperator> o1NewUnifiers = plan.getMergeOperators(o1Meta);
    Assert.assertEquals("unifiers " + o1Meta, 0, o1NewUnifiers.size());
    p1Doper = o1p1.getOutputs().get(0).sinks.get(0).target;
    Assert.assertTrue("", p1Doper.getOperatorMeta() == dag.getMeta(o2));
    Assert.assertNull("unifier ", p1Doper.getUnifier());

    Assert.assertTrue("removed unifier from deployment " + ctx.undeploy,  ctx.undeploy.containsAll(o1Unifiers));
    Assert.assertFalse("removed unifier from deployment " + ctx.deploy,  ctx.deploy.containsAll(o1Unifiers));

  }

  @Test
  public void testDefaultRepartitioning() {

    List<PartitionKeys> twoBitPartitionKeys = Arrays.asList(
            newPartitionKeys("11", "00"),
            newPartitionKeys("11", "10"),
            newPartitionKeys("11", "01"),
            newPartitionKeys("11", "11"));

    OperatorPartitions.DefaultPartitioner dp = new OperatorPartitions.DefaultPartitioner();
    GenericTestOperator operator = new GenericTestOperator();

    Set<PartitionKeys> initialPartitionKeys = Sets.newHashSet(
            newPartitionKeys("1", "0"),
            newPartitionKeys("1", "1"));

    final ArrayList<Partition<?>> partitions = new ArrayList<Partition<?>>();
    for (PartitionKeys pks: initialPartitionKeys) {
      Map<InputPort<?>, PartitionKeys> p1Keys = new HashMap<InputPort<?>, PartitionKeys>();
      p1Keys.put(operator.inport1, pks);
      partitions.add(new PartitionImpl(operator, p1Keys, 1));
    }

    ArrayList<Partition<?>> lowLoadPartitions = new ArrayList<Partition<?>>();
    for (Partition<?> p : partitions) {
      lowLoadPartitions.add(new PartitionImpl(p.getOperator(), p.getPartitionKeys(), -1));
    }
    // merge to single partition
    List<Partition<?>> newPartitions = dp.repartition(lowLoadPartitions);
    Assert.assertEquals("" + newPartitions, 1, newPartitions.size());
    Assert.assertEquals("" + newPartitions.get(0).getPartitionKeys(), 0, newPartitions.get(0).getPartitionKeys().values().iterator().next().mask);

    newPartitions = dp.repartition(Collections.singletonList(new PartitionImpl(operator, newPartitions.get(0).getPartitionKeys(), -1)));
    Assert.assertEquals("" + newPartitions, 1, newPartitions.size());

    // split back into two
    newPartitions = dp.repartition(Collections.singletonList(new PartitionImpl(operator, newPartitions.get(0).getPartitionKeys(), 1)));
    Assert.assertEquals("" + newPartitions, 2, newPartitions.size());


    // split partitions
    newPartitions = dp.repartition(partitions);
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
    @SuppressWarnings("unchecked")
    List<HashSet<PartitionKeys>> expectedKeysSets = Arrays.asList(
        Sets.newHashSet(
          newPartitionKeys("11", "00"),
          newPartitionKeys("11", "10"),
          newPartitionKeys("1", "1")
        ),
        Sets.newHashSet(
          newPartitionKeys("1", "0"),
          newPartitionKeys("11", "01"),
          newPartitionKeys("11", "11")
        )
    );

    for (Set<PartitionKeys> expectedKeys: expectedKeysSets) {
      List<Partition<?>> clonePartitions = Lists.newArrayList();
      for (PartitionKeys pks: twoBitPartitionKeys) {
        Map<InputPort<?>, PartitionKeys> p1Keys = new HashMap<InputPort<?>, PartitionKeys>();
        p1Keys.put(operator.inport1, pks);
        int load = expectedKeys.contains(pks) ? 0 : -1;
        clonePartitions.add(new PartitionImpl(operator, p1Keys, load));
      }

      newPartitions = dp.repartition(clonePartitions);
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
      lowLoadPartitions.add(new PartitionImpl(operator, p.getPartitionKeys(), -1));
    }
    newPartitions = dp.repartition(lowLoadPartitions);
    Assert.assertEquals("" + newPartitions, 1, newPartitions.size());
    for (Partition<?> p: newPartitions) {
      Assert.assertEquals("" + p.getPartitionKeys(), 1, p.getPartitionKeys().size());
      PartitionKeys pks = p.getPartitionKeys().values().iterator().next();
      Assert.assertEquals("" + pks, 0, pks.mask);
      Assert.assertEquals("" + pks, Sets.newHashSet(0), pks.partitions);
    }
  }

  private PartitionKeys newPartitionKeys(String mask, String key) {
    return new PartitionKeys(Integer.parseInt(mask, 2), Sets.newHashSet(Integer.parseInt(key, 2)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInline() {

    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    PartitioningTestOperator partNode = dag.addOperator("partNode", PartitioningTestOperator.class);
    partNode.partitionKeys = new Integer[] {0,1};
    dag.getMeta(partNode).getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, partNode.partitionKeys.length);

    dag.addStream("o1_outport1", o1.outport1, o2.inport1, o3.inport1, partNode.inport1)
            .setLocality(null);

    // same container for o2 and o3
    dag.addStream("o2_outport1", o2.outport1, o3.inport2)
            .setLocality(Locality.CONTAINER_LOCAL);

    dag.addStream("o3_outport1", o3.outport1, partNode.inport2);

    int maxContainers = 4;
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);
    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", maxContainers, plan.getContainers().size());
    Assert.assertEquals("operators container 0", 1, plan.getContainers().get(0).getOperators().size());

    Assert.assertEquals("operators container 0", 1, plan.getContainers().get(0).getOperators().size());
    Set<OperatorMeta> c2ExpNodes = Sets.newHashSet(dag.getMeta(o2), dag.getMeta(o3));
    Set<OperatorMeta> c2ActNodes = new HashSet<OperatorMeta>();
    PTContainer c2 = plan.getContainers().get(1);
    for (PTOperator pNode: c2.getOperators()) {
      c2ActNodes.add(pNode.getOperatorMeta());
    }
    Assert.assertEquals("operators " + c2, c2ExpNodes, c2ActNodes);

    // one container per partition
    OperatorMeta partOperMeta = dag.getMeta(partNode);
    List<PTOperator> partitions = plan.getOperators(partOperMeta);
    for (PTOperator partition : partitions) {
      Assert.assertEquals("operators container" + partition, 1, partition.getContainer().getOperators().size());
    }

  }

  @Test
  public void testInlineMultipleInputs() {

    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);

    dag.addStream("n1Output1", node1.outport1, node3.inport1)
            .setLocality(Locality.CONTAINER_LOCAL);

    dag.addStream("n2Output1", node2.outport1, node3.inport2)
            .setLocality(Locality.CONTAINER_LOCAL);

    int maxContainers = 5;
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);

    PhysicalPlan deployer = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", 1, deployer.getContainers().size());

    PTOutput node1Out = deployer.getOperators(dag.getMeta(node1)).get(0).getOutputs().get(0);
    Assert.assertTrue("inline " + node1Out, node1Out.isDownStreamInline());

    // per current logic, different container is assigned to second input node
    PTOutput node2Out = deployer.getOperators(dag.getMeta(node2)).get(0).getOutputs().get(0);
    Assert.assertTrue("inline " + node2Out, node2Out.isDownStreamInline());

  }

  @Test
  public void testNodeLocality() {

    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    GenericTestOperator partitioned = dag.addOperator("partitioned", GenericTestOperator.class);
    dag.getMeta(partitioned).getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, 2);

    GenericTestOperator partitionedParallel = dag.addOperator("partitionedParallel", GenericTestOperator.class);

    dag.addStream("o1_outport1", o1.outport1, partitioned.inport1).setLocality(null);

    dag.addStream("partitioned_outport1", partitioned.outport1, partitionedParallel.inport2).setLocality(Locality.NODE_LOCAL);
    dag.setInputPortAttribute(partitionedParallel.inport2, PortContext.PARTITION_PARALLEL, true);

    GenericTestOperator single = dag.addOperator("single", GenericTestOperator.class);
    dag.addStream("partitionedParallel_outport1", partitionedParallel.outport1, single.inport1);

    int maxContainers = 7;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);

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
      for (PTOperator p: c.getOperators()) {
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
      for (PTOperator p: c.getOperators()) {
        actualLogical.add(p.getOperatorMeta());
        Assert.assertEquals("nodeLocal " + p.getNodeLocalOperators(), 2, p.getNodeLocalOperators().getOperatorSet().size());
      }
      Assert.assertEquals("operators " + c, expectedLogical, actualLogical);
    }
  }

  @Test
  public void testParallelPartitioning() {

    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.getMeta(o2).getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, 2);

    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("o1Output1", o1.outport1, o2.inport1, o3.inport1).setLocality(null);

    dag.addStream("o2Output1", o2.outport1, o3.inport2).setLocality(Locality.CONTAINER_LOCAL);
    dag.setInputPortAttribute(o3.inport2, PortContext.PARTITION_PARALLEL, true);

    // parallel partition two downstream operators
    GenericTestOperator o3_1 = dag.addOperator("o3_1", GenericTestOperator.class);
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

    int maxContainers = 5;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", 5, plan.getContainers().size());

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

    // container 4: merge operator for o4
    Collection<PTOperator> o4Unifiers = plan.getMergeOperators(o4Meta);
    Assert.assertEquals("unifier " + o4Meta + ": " + o4Unifiers, 1, o4Unifiers.size());
    PTContainer container4 = plan.getContainers().get(3);
    Assert.assertEquals("number operators " + container4, 1, container4.getOperators().size());
    Assert.assertEquals("operators " + container4, o4Meta, container4.getOperators().get(0).getOperatorMeta());
    Assert.assertTrue("unifier " + o4, container4.getOperators().get(0).getUnifier() instanceof Unifier);
    Assert.assertEquals("unifier inputs" + container4.getOperators().get(0).getInputs(), 2, container4.getOperators().get(0).getInputs().size());
    Assert.assertEquals("unifier outputs" + container4.getOperators().get(0).getOutputs(), 1, container4.getOperators().get(0).getOutputs().size());

    // container 5: o5 taking input from o4 unifier
    OperatorMeta o5Meta = dag.getMeta(o5single);
    PTContainer container5 = plan.getContainers().get(4);
    Assert.assertEquals("number operators " + container5, 1, container5.getOperators().size());
    Assert.assertEquals("operators " + container5, o5Meta, container5.getOperators().get(0).getOperatorMeta());
    List<PTOperator> o5Instances = plan.getOperators(o5Meta);
    Assert.assertEquals("" + o5Instances, 1, o5Instances.size());
    Assert.assertEquals("inputs" + container5.getOperators().get(0).getInputs(), 1, container5.getOperators().get(0).getInputs().size());
    Assert.assertEquals("inputs" + container5.getOperators().get(0).getInputs(), container4.getOperators().get(0), container5.getOperators().get(0).getInputs().get(0).source.source);

  }

  /**
   * MxN partitioning. When source and sink of a stream are partitioned, a
   * separate unifier is created container local with each downstream partition.
   */
  @Test
  public void testUnifierPartitioning() {

    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    dag.setAttribute(o1, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    OperatorMeta o1Meta = dag.getMeta(o1);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setAttribute(o2, OperatorContext.INITIAL_PARTITION_COUNT, 3);
    OperatorMeta o2Meta = dag.getMeta(o2);

    dag.addStream("o1.outport1", o1.outport, o2.inport1);

    int maxContainers = 10;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", 5, plan.getContainers().size());

    List<PTOperator> inputOperators = new ArrayList<PTOperator>();
    for (int i=0; i<2; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertEquals("operators " + container, o1Meta.getName(), container.getOperators().get(0).getOperatorMeta().getName());
      inputOperators.add(container.getOperators().get(0));
    }

    for (int i=2; i<5; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 2, container.getOperators().size());
      Assert.assertEquals("operators " + container, o2Meta.getName(), container.getOperators().get(0).getOperatorMeta().getName());
      Set<String> expectedLogicalNames = Sets.newHashSet(o1Meta.getName(), o2Meta.getName());
      Map<String, PTOperator> actualOperators = new HashMap<String, PTOperator>();
      for (PTOperator p : container.getOperators()) {
        actualOperators.put(p.getOperatorMeta().getName(), p);
      }
      Assert.assertEquals("", expectedLogicalNames, actualOperators.keySet());

      PTOperator pUnifier = actualOperators.get(o1Meta.getName());
      Assert.assertNotNull("" + pUnifier, pUnifier.getContainer());
      Assert.assertTrue("" + pUnifier, pUnifier.getUnifier() instanceof Unifier);
      // input from each upstream partition
      Assert.assertEquals("" + pUnifier, 2, pUnifier.getInputs().size());
      for (int inputIndex = 0; i < pUnifier.getInputs().size(); i++) {
        PTInput input = pUnifier.getInputs().get(inputIndex);
        Assert.assertEquals("" + pUnifier, "outputPort", input.source.portName);
        Assert.assertEquals("" + pUnifier, inputOperators.get(inputIndex), input.source.source);
        Assert.assertEquals("partition keys " + input.partitions, 1, input.partitions.partitions.size());
      }
      // output to single downstream partition
      Assert.assertEquals("" + pUnifier, 1, pUnifier.getOutputs().size());
      Assert.assertTrue(""+actualOperators.get(o2Meta.getName()).getOperatorMeta().getOperator(), actualOperators.get(o2Meta.getName()).getOperatorMeta().getOperator() instanceof GenericTestOperator);

      PTOperator p = actualOperators.get(o2Meta.getName());
      Assert.assertEquals("partition inputs " + p.getInputs(), 1, p.getInputs().size());
      Assert.assertEquals("partition inputs " + p.getInputs(), pUnifier, p.getInputs().get(0).source.source);
      Assert.assertEquals("input partition keys " + p.getInputs(), null, p.getInputs().get(0).partitions);
      Assert.assertTrue("partitioned unifier container local " + p.getInputs().get(0).source, p.getInputs().get(0).source.isDownStreamInline());
    }
  }

  @Test
  public void testCascadingUnifier() {

    LogicalPlan dag = new LogicalPlan();

    //TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    PartitioningTestOperator o1 = dag.addOperator("o1", PartitioningTestOperator.class);
    o1.partitionKeys = new Integer[] {0,1,2,3};

    dag.setAttribute(o1, OperatorContext.INITIAL_PARTITION_COUNT, o1.partitionKeys.length);
    dag.setAttribute(o1, OperatorContext.PARTITION_STATS_HANDLER, PartitioningTest.PartitionLoadWatch.class.getName());

    dag.setOutputPortAttribute(o1.outport1, PortContext.UNIFIER_LIMIT, 2);
    OperatorMeta o1Meta = dag.getMeta(o1);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setAttribute(o2, OperatorContext.INITIAL_PARTITION_COUNT, 3);
    OperatorMeta o2Meta = dag.getMeta(o2);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 10);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    Assert.assertEquals("number of containers", 9, plan.getContainers().size());

    List<PTOperator> o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1Meta, 4, o1Partitions.size());
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
          Assert.assertNotNull(in.target.getUnifier());
          Assert.assertEquals(1, in.target.getOutputs().get(0).sinks.size());
        }
      }
      Assert.assertNotNull("container " + o, o.getContainer());
    }

    for (int i=0; i<4; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertTrue(o1Partitions.contains(container.getOperators().get(0)));
    }

    for (int i=5; i<6; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.getOperators().size());
      Assert.assertTrue(o1Unifiers.contains(container.getOperators().get(0)));
    }

    for (int i=6; i<8; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 2, container.getOperators().size());
      Assert.assertTrue(o2Partitions.contains(container.getOperators().get(0)));
    }

    PTOperator p1 = o1Partitions.get(0);
    PhysicalPlan.StatsHandler sm = p1.statsMonitors.get(0);
    Assert.assertTrue("stats handlers " + p1.statsMonitors, sm instanceof PartitioningTest.PartitionLoadWatch);
    PartitioningTest.PartitionLoadWatch.loadIndicators.put(p1, 1);

    sm.onThroughputUpdate(p1, 1);
    Assert.assertEquals("partition scaling triggered", 1, ctx.events.size());

    o1.partitionKeys = new Integer[] {0,1,2,3,4};
    ctx.events.remove(0).run();

    o1Partitions = plan.getOperators(o1Meta);
    Assert.assertEquals("partitions " + o1Meta, 5, o1Partitions.size());

    o1Unifiers = plan.getMergeOperators(o1Meta);
    Assert.assertEquals("o1Unifiers " + o1Meta, 5, o1Unifiers.size()); // 3(l1)x2(l2)
    for (PTOperator o : o1Unifiers) {
      Assert.assertNotNull("container null: " + o, o.getContainer());
    }

  }

}

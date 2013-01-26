/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Sets;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.api.DAG.OperatorWrapper;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.OperatorCodec;
import com.malhartech.api.PartitionableOperator;
import com.malhartech.api.PartitionableOperator.Partition;
import com.malhartech.api.PartitionableOperator.PartitionKeys;
import com.malhartech.api.StreamCodec;
import com.malhartech.engine.DefaultStreamCodec;
import com.malhartech.engine.GenericTestModule;
import com.malhartech.stram.OperatorPartitions.PartitionImpl;
import com.malhartech.stram.PartitioningTest.PartitionableInputOperator;
import com.malhartech.stram.PhysicalPlan.PTContainer;
import com.malhartech.stram.PhysicalPlan.PTInput;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.PhysicalPlan.PTOutput;
import com.malhartech.stram.PhysicalPlan.PlanContext;

public class PhysicalPlanTest {

  public static class PartitioningTestStreamCodec extends DefaultStreamCodec<Object> {
    @Override
    public int getPartition(Object o) {
      return PartitioningTestOperator.PARTITION_KEYS[ o.hashCode() % PartitioningTestOperator.PARTITION_KEYS.length ];
    }
  }

  public static class PartitioningTestOperator extends GenericTestModule implements PartitionableOperator {
    final static Integer[] PARTITION_KEYS = { 0, 1, 2};
    final static String INPORT_WITH_CODEC = "inportWithCodec";

    @InputPortFieldAnnotation(name=INPORT_WITH_CODEC, optional=true)
    final public transient InputPort<Object> inportWithCodec = new DefaultInputPort<Object>(this) {

      @Override
      public Class<? extends StreamCodec<Object>> getStreamCodec() {
        return PartitioningTestStreamCodec.class;
      }

      @Override
      final public void process(Object payload) {
      }
    };

    @Override
    public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacity)
    {
      List<Partition<?>> newPartitions = new ArrayList<Partition<?>>(3);
      @SuppressWarnings("unchecked")
      Partition<PartitioningTestOperator> templatePartition = (Partition<PartitioningTestOperator>)partitions.iterator().next();
      for (int i=0; i<3; i++) {
        Partition<PartitioningTestOperator> p = templatePartition.getInstance(new PartitioningTestOperator());
        p.getPartitionKeys().put(this.inport1, new PartitionKeys(0, Sets.newHashSet(PARTITION_KEYS[i])));
        p.getPartitionKeys().put(this.inportWithCodec, new PartitionKeys(0, Sets.newHashSet(PARTITION_KEYS[i])));
        newPartitions.add(p);
      }
      return newPartitions;
    }

  }

  @Test
  public void testStaticPartitioning() {
    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    PartitioningTestOperator node2 = dag.addOperator("node2", PartitioningTestOperator.class);
    GenericTestModule mergeNode = dag.addOperator("mergeNode", GenericTestModule.class);

    dag.addStream("n1.outport1", node1.outport1, node2.inport1, node2.inportWithCodec);
    dag.addStream("mergeStream", node2.outport1, mergeNode.inport1);

    dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(2);

    OperatorWrapper node2Decl = dag.getOperatorWrapper(node2.getName());

    PhysicalPlan plan = new PhysicalPlan(dag, null);

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());

    List<PTOperator> n2Instances = plan.getOperators(node2Decl);
    Assert.assertEquals("partition instances " + n2Instances, PartitioningTestOperator.PARTITION_KEYS.length, n2Instances.size());
    for (int i=0; i<PartitioningTestOperator.PARTITION_KEYS.length; i++) {
      PTOperator po = n2Instances.get(i);
      Map<String, PTInput> inputsMap = new HashMap<String, PTInput>();
      for (PTInput input : po.inputs) {
        inputsMap.put(input.portName, input);
        Assert.assertEquals("partitions "+input, Sets.newHashSet(PartitioningTestOperator.PARTITION_KEYS[i]), input.partitions.partitions);
        Assert.assertEquals("codec " + input.logicalStream, PartitioningTestStreamCodec.class, input.logicalStream.getCodecClass());
      }
      Assert.assertEquals("number inputs " + inputsMap, Sets.newHashSet(PartitioningTestOperator.IPORT1, PartitioningTestOperator.INPORT_WITH_CODEC), inputsMap.keySet());
    }
  }

  @Test
  public void testDefaultPartitioning() {
    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);
    dag.addStream("node1.outport1", node1.outport1, node2.inport2, node2.inport1);

    int initialPartitionCount = 5;
    OperatorWrapper node2Decl = dag.getOperatorWrapper(node2.getName());
    node2Decl.getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(initialPartitionCount);

    PhysicalPlan plan = new PhysicalPlan(dag, null);

    List<PTOperator> n2Instances = plan.getOperators(node2Decl);
    Assert.assertEquals("partition instances " + n2Instances, initialPartitionCount, n2Instances.size());

    for (int i=0; i<n2Instances.size(); i++) {
      PTOperator partitionInstance = n2Instances.get(i);
      Partition<?> p = partitionInstance.partition;
      Assert.assertNotNull("partition null: " + partitionInstance, p);
      Map<InputPort<?>, PartitionKeys> pkeys = p.getPartitionKeys();
      Assert.assertNotNull("partition keys null: " + partitionInstance, pkeys);
      Assert.assertEquals("partition keys size: " + pkeys, 1, pkeys.size()); // one port partitioned
      // default partitioning does not clone the operator
      Assert.assertEquals("partition operator: " + pkeys, node2, partitionInstance.partition.getOperator());
      InputPort<?> expectedPort = node2.inport2;
      Assert.assertEquals("partition port: " + pkeys, expectedPort, pkeys.keySet().iterator().next());

      Assert.assertEquals("partition mask: " + pkeys, "111", Integer.toBinaryString(pkeys.get(expectedPort).mask));
      Assert.assertEquals("partition id: " + pkeys, Sets.newHashSet(i), pkeys.get(expectedPort).partitions);
    }

  }

  private class TestPlanContext implements PlanContext, BackupAgent {
    List<Runnable> events = new ArrayList<Runnable>();
    Set<PTOperator> deps;
    Collection<PTOperator> undeploy;
    Collection<PTOperator> deploy;
    List<Object> backupRequests = new ArrayList<Object>();


    @Override
    public BackupAgent getBackupAgent() {
      return this;
    }

    @Override
    public void redeploy(Collection<PTOperator> undeploy, Set<PTContainer> startContainers, Collection<PTOperator> deploy) {
      this.undeploy = undeploy;
      this.deploy = deploy;
    }

    @Override
    public Set<PTOperator> getDependents(Collection<PTOperator> p) {
      Assert.assertNotNull("dependencies not set", deps);
      return deps;
    }

    @Override
    public void dispatch(Runnable r) {
      events.add(r);
    }

    @Override
    public void backup(int operatorId, long windowId, Object o, OperatorCodec serDe) throws IOException {
      backupRequests.add(o);
    }

    @Override
    public Object restore(int operatorId, long windowId, OperatorCodec serDe) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete(int operatorId, long windowId) throws IOException {
      throw new UnsupportedOperationException();
    }

  }

  @Test
  public void testRepartitioning() {
    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);
    GenericTestModule mergeNode = dag.addOperator("mergeNode", GenericTestModule.class);

    dag.addStream("n1.outport1", node1.outport1, node2.inport1, node2.inport2);
    dag.addStream("mergeStream", node2.outport1, mergeNode.inport1);

    dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(2);

    OperatorWrapper node2Decl = dag.getOperatorWrapper(node2.getName());
    node2Decl.getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(2);
    node2Decl.getAttributes().attr(OperatorContext.PARTITION_TPS_MIN).set(0);
    node2Decl.getAttributes().attr(OperatorContext.PARTITION_TPS_MAX).set(5);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    ctx.deps = Sets.newHashSet(plan.getOperators(dag.getOperatorWrapper(mergeNode)));

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());

    List<PTOperator> n2Instances = plan.getOperators(node2Decl);
    Assert.assertEquals("partition instances " + n2Instances, 2, n2Instances.size());

    // verify load update generates expected events per configuration
    PTOperator po = n2Instances.get(0);
    Assert.assertEquals("stats handlers " + po, 1, po.statsMonitors.size());
    PhysicalPlan.StatsHandler sm = po.statsMonitors.get(0);
    Assert.assertTrue("stats handlers " + po.statsMonitors, sm instanceof PhysicalPlan.PartitionLoadWatch);
    sm.onThroughputUpdate(po, 0);
    Assert.assertEquals("load event triggered", 0, ctx.events.size());
    sm.onThroughputUpdate(po, 3);
    Assert.assertEquals("load within range", 0, ctx.events.size());
    sm.onThroughputUpdate(po, 10);
    Assert.assertEquals("load exceeds max", 1, ctx.events.size());

    Runnable r = ctx.events.remove(0);
    r.run();

    Assert.assertEquals("" + ctx.undeploy, ctx.deps, ctx.undeploy);
    Assert.assertEquals("" + ctx.deploy, ctx.deps, ctx.deploy);
    Assert.assertEquals("backup " + ctx.backupRequests, 2, ctx.backupRequests.size());

  }

  @Test
  public void testDefaultRepartitioning() {

    List<PartitionKeys> twoBitPartitionKeys = Arrays.asList(
        newPartitionKeys("11", "00"),
        newPartitionKeys("11", "10"),
        newPartitionKeys("11", "01"),
        newPartitionKeys("11", "11")
    );

    OperatorPartitions.DefaultPartitioner dp = new OperatorPartitions.DefaultPartitioner();
    GenericTestModule operator = new GenericTestModule();

    Set<PartitionKeys> initialPartitionKeys = Sets.newHashSet(
        newPartitionKeys("1", "0"),
        newPartitionKeys("1", "1")
    );

    ArrayList<Partition<?>> partitions = new ArrayList<Partition<?>>();
    for (PartitionKeys pks : initialPartitionKeys) {
      Map<InputPort<?>, PartitionKeys> p1Keys = new HashMap<InputPort<?>, PartitionKeys>();
      p1Keys.put(operator.inport1, pks);
      partitions.add(new PartitionImpl(operator, p1Keys, 1));
    }

    List<Partition<?>> newPartitions = dp.repartition(partitions);
    Assert.assertEquals(""+newPartitions, 4, newPartitions.size());

    Set<PartitionKeys> expectedPartitionKeys = Sets.newHashSet(twoBitPartitionKeys);
    for (Partition<?> p : newPartitions) {
      Assert.assertEquals(""+p.getPartitionKeys(), 1, p.getPartitionKeys().size());
      Assert.assertEquals(""+p.getPartitionKeys(), operator.inport1, p.getPartitionKeys().keySet().iterator().next());
      PartitionKeys pks = p.getPartitionKeys().values().iterator().next();
      expectedPartitionKeys.remove(pks);
    }
    Assert.assertTrue("" + expectedPartitionKeys, expectedPartitionKeys.isEmpty());

    // partition merge
    @SuppressWarnings("unchecked")
    List<HashSet<PartitionKeys>> mergedKeys = Arrays.asList(
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

    for (Set<PartitionKeys> expectedKeys : mergedKeys) {
      partitions = new ArrayList<Partition<?>>();
      for (PartitionKeys pks : twoBitPartitionKeys) {
        Map<InputPort<?>, PartitionKeys> p1Keys = new HashMap<InputPort<?>, PartitionKeys>();
        p1Keys.put(operator.inport1, pks);
        int load = expectedKeys.contains(pks) ? 0 : -1;
        partitions.add(new PartitionImpl(operator, p1Keys, load));
      }

      newPartitions = dp.repartition(partitions);
      Assert.assertEquals(""+newPartitions, 3, newPartitions.size());

      for (Partition<?> p : newPartitions) {
        Assert.assertEquals(""+p.getPartitionKeys(), 1, p.getPartitionKeys().size());
        Assert.assertEquals(""+p.getPartitionKeys(), operator.inport1, p.getPartitionKeys().keySet().iterator().next());
        PartitionKeys pks = p.getPartitionKeys().values().iterator().next();
        expectedKeys.remove(pks);
      }
      Assert.assertTrue("" + expectedKeys, expectedKeys.isEmpty());
    }

  }

  private PartitionKeys newPartitionKeys(String mask, String key) {
    return new PartitionKeys(Integer.parseInt(mask,2), Sets.newHashSet(Integer.parseInt(key,2)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInline() {

    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);
    GenericTestModule node3 = dag.addOperator("node3", GenericTestModule.class);

    GenericTestModule notInlineNode = dag.addOperator("notInlineNode", GenericTestModule.class);
    // partNode has 2 inputs, inline must be ignored with partitioned input
    PartitioningTestOperator partNode = dag.addOperator("partNode", PartitioningTestOperator.class);

    dag.addStream("n1Output1", node1.outport1, node2.inport1, node3.inport1, partNode.inport1)
      .setInline(true);

    dag.addStream("n2Output1", node2.outport1, node3.inport2, notInlineNode.inport1)
      .setInline(false);

    dag.addStream("n3Output1", node3.outport1, partNode.inport2);

    int maxContainers = 5;
    dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(maxContainers);
    PhysicalPlan deployer1 = new PhysicalPlan(dag, null);
    Assert.assertEquals("number of containers", maxContainers, deployer1.getContainers().size());
    Assert.assertEquals("operators container 0", 3, deployer1.getContainers().get(0).operators.size());

    Set<OperatorWrapper> c1ExpNodes = Sets.newHashSet(dag.getOperatorWrapper(node1.getName()), dag.getOperatorWrapper(node2.getName()), dag.getOperatorWrapper(node3.getName()));
    Set<OperatorWrapper> c1ActNodes = new HashSet<OperatorWrapper>();
    for (PTOperator pNode : deployer1.getContainers().get(0).operators) {
      c1ActNodes.add(pNode.getLogicalNode());
    }
    Assert.assertEquals("operators container 0", c1ExpNodes, c1ActNodes);

    Assert.assertEquals("operators container 1", 1, deployer1.getContainers().get(1).operators.size());
    Assert.assertEquals("operators container 1", dag.getOperatorWrapper(notInlineNode.getName()), deployer1.getContainers().get(1).operators.get(0).getLogicalNode());

    // one container per partition
    for (int cindex = 2; cindex < maxContainers; cindex++) {
      Assert.assertEquals("operators container" + cindex, 1, deployer1.getContainers().get(cindex).operators.size());
      Assert.assertEquals("operators container" + cindex, dag.getOperatorWrapper(partNode.getName()), deployer1.getContainers().get(cindex).operators.get(0).getLogicalNode());
    }

  }

  @Test
  public void testInlineMultipleInputs() {

    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);
    GenericTestModule node3 = dag.addOperator("node3", GenericTestModule.class);

    dag.addStream("n1Output1", node1.outport1, node3.inport1)
      .setInline(true);

    dag.addStream("n2Output1", node2.outport1, node3.inport2)
      .setInline(true);

    int maxContainers = 5;
    dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(maxContainers);

    PhysicalPlan deployer = new PhysicalPlan(dag, null);
    Assert.assertEquals("number of containers", 1, deployer.getContainers().size());

    PTOutput node1Out = deployer.getOperators(dag.getOperatorWrapper(node1)).get(0).outputs.get(0);
    Assert.assertTrue("inline " + node1Out, node1Out.isDownStreamInline());

    // per current logic, different container is assigned to second input node
    PTOutput node2Out = deployer.getOperators(dag.getOperatorWrapper(node2)).get(0).outputs.get(0);
    Assert.assertTrue("inline " + node2Out, node2Out.isDownStreamInline());

  }

}

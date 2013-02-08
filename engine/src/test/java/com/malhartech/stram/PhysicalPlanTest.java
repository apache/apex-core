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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Context.PortContext;
import com.malhartech.api.DAG;
import com.malhartech.api.DAG.OperatorMeta;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.api.OperatorCodec;
import com.malhartech.api.PartitionableOperator;
import com.malhartech.api.PartitionableOperator.Partition;
import com.malhartech.api.PartitionableOperator.PartitionKeys;
import com.malhartech.api.StreamCodec;
import com.malhartech.engine.DefaultStreamCodec;
import com.malhartech.engine.GenericTestModule;
import com.malhartech.engine.TestGeneratorInputModule;
import com.malhartech.stram.OperatorPartitions.PartitionImpl;
import com.malhartech.stram.PhysicalPlan.PTContainer;
import com.malhartech.stram.PhysicalPlan.PTInput;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.PhysicalPlan.PTOutput;
import com.malhartech.stram.PhysicalPlan.PlanContext;

public class PhysicalPlanTest {
  public static class PartitioningTestStreamCodec extends DefaultStreamCodec<Object> {
    @Override
    public int getPartition(Object o) {
      return PartitioningTestOperator.PARTITION_KEYS[ o.hashCode() % PartitioningTestOperator.PARTITION_KEYS.length];
    }

  }

  public static class PartitioningTestOperator extends GenericTestModule implements PartitionableOperator {
    final static Integer[] PARTITION_KEYS = {0, 1, 2};
    final static String INPORT_WITH_CODEC = "inportWithCodec";
    @InputPortFieldAnnotation(name = INPORT_WITH_CODEC, optional = true)
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
    @SuppressWarnings("unchecked")
    public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacity) {
      incrementalCapacity += partitions.size();
      if (incrementalCapacity == partitions.size()) {
        return (Collection<Partition<?>>)partitions;
      }
      else {
        List<Partition<?>> newPartitions = new ArrayList<Partition<?>>(3);
        Partition<PartitioningTestOperator> templatePartition = (Partition<PartitioningTestOperator>)partitions.iterator().next();
        for (int i = 0; i < incrementalCapacity; i++) {
          Partition<PartitioningTestOperator> p = templatePartition.getInstance(new PartitioningTestOperator());
          p.getPartitionKeys().put(this.inport1, new PartitionKeys(0, Sets.newHashSet(PARTITION_KEYS[i])));
          p.getPartitionKeys().put(this.inportWithCodec, new PartitionKeys(0, Sets.newHashSet(PARTITION_KEYS[i])));
          newPartitions.add(p);
        }
        return newPartitions;
      }
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

    dag.getOperatorWrapper(node2).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(3);
    dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(2);

    OperatorMeta node2Decl = dag.getOperatorWrapper(node2.getName());

    PhysicalPlan plan = new PhysicalPlan(dag, null);

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());

    List<PTOperator> n2Instances = plan.getOperators(node2Decl);
    Assert.assertEquals("partition instances " + n2Instances, PartitioningTestOperator.PARTITION_KEYS.length, n2Instances.size());
    for (int i = 0; i < PartitioningTestOperator.PARTITION_KEYS.length; i++) {
      PTOperator po = n2Instances.get(i);
      Map<String, PTInput> inputsMap = new HashMap<String, PTInput>();
      for (PTInput input: po.inputs) {
        inputsMap.put(input.portName, input);
        Assert.assertEquals("partitions " + input, Sets.newHashSet(PartitioningTestOperator.PARTITION_KEYS[i]), input.partitions.partitions);
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
    OperatorMeta node2Decl = dag.getOperatorWrapper(node2.getName());
    node2Decl.getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(initialPartitionCount);

    PhysicalPlan plan = new PhysicalPlan(dag, null);

    List<PTOperator> n2Instances = plan.getOperators(node2Decl);
    Assert.assertEquals("partition instances " + n2Instances, initialPartitionCount, n2Instances.size());

    List<Integer> assignedPartitionKeys = Lists.newArrayList();

    for (int i = 0; i < n2Instances.size(); i++) {
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

    OperatorMeta node2Decl = dag.getOperatorWrapper(node2.getName());
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
            newPartitionKeys("11", "11"));

    OperatorPartitions.DefaultPartitioner dp = new OperatorPartitions.DefaultPartitioner();
    GenericTestModule operator = new GenericTestModule();

    Set<PartitionKeys> initialPartitionKeys = Sets.newHashSet(
            newPartitionKeys("1", "0"),
            newPartitionKeys("1", "1"));

    ArrayList<Partition<?>> partitions = new ArrayList<Partition<?>>();
    for (PartitionKeys pks: initialPartitionKeys) {
      Map<InputPort<?>, PartitionKeys> p1Keys = new HashMap<InputPort<?>, PartitionKeys>();
      p1Keys.put(operator.inport1, pks);
      partitions.add(new PartitionImpl(operator, p1Keys, 1));
    }

    List<Partition<?>> newPartitions = dp.repartition(partitions);
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
      partitions = new ArrayList<Partition<?>>();
      for (PartitionKeys pks: twoBitPartitionKeys) {
        Map<InputPort<?>, PartitionKeys> p1Keys = new HashMap<InputPort<?>, PartitionKeys>();
        p1Keys.put(operator.inport1, pks);
        int load = expectedKeys.contains(pks) ? 0 : -1;
        partitions.add(new PartitionImpl(operator, p1Keys, load));
      }

      newPartitions = dp.repartition(partitions);
      Assert.assertEquals("" + newPartitions, 3, newPartitions.size());

      for (Partition<?> p: newPartitions) {
        Assert.assertEquals("" + p.getPartitionKeys(), 1, p.getPartitionKeys().size());
        Assert.assertEquals("" + p.getPartitionKeys(), operator.inport1, p.getPartitionKeys().keySet().iterator().next());
        PartitionKeys pks = p.getPartitionKeys().values().iterator().next();
        expectedKeys.remove(pks);
      }
      Assert.assertTrue("" + expectedKeys, expectedKeys.isEmpty());
    }

  }

  private PartitionKeys newPartitionKeys(String mask, String key) {
    return new PartitionKeys(Integer.parseInt(mask, 2), Sets.newHashSet(Integer.parseInt(key, 2)));
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
    dag.getOperatorWrapper(partNode).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(3);

    dag.addStream("n1Output1", node1.outport1, node2.inport1, node3.inport1, partNode.inport1)
            .setInline(true);

    dag.addStream("n2Output1", node2.outport1, node3.inport2, notInlineNode.inport1)
            .setInline(false);

    dag.addStream("n3Output1", node3.outport1, partNode.inport2);

    int maxContainers = 5;
    dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(maxContainers);
    PhysicalPlan plan = new PhysicalPlan(dag, null);
    Assert.assertEquals("number of containers", maxContainers, plan.getContainers().size());
    Assert.assertEquals("operators container 0", 3, plan.getContainers().get(0).operators.size());

    Set<OperatorMeta> c1ExpNodes = Sets.newHashSet(dag.getOperatorWrapper(node1.getName()), dag.getOperatorWrapper(node2.getName()), dag.getOperatorWrapper(node3.getName()));
    Set<OperatorMeta> c1ActNodes = new HashSet<OperatorMeta>();
    for (PTOperator pNode: plan.getContainers().get(0).operators) {
      c1ActNodes.add(pNode.getLogicalNode());
    }
    Assert.assertEquals("operators container 0", c1ExpNodes, c1ActNodes);

    Assert.assertEquals("operators container 1", 1, plan.getContainers().get(1).operators.size());
    Assert.assertEquals("operators container 1", dag.getOperatorWrapper(notInlineNode.getName()), plan.getContainers().get(1).operators.get(0).getLogicalNode());

    // one container per partition
    for (int cindex = 2; cindex < maxContainers; cindex++) {
      Assert.assertEquals("operators container" + cindex, 1, plan.getContainers().get(cindex).operators.size());
      Assert.assertEquals("operators container" + cindex, dag.getOperatorWrapper(partNode.getName()), plan.getContainers().get(cindex).operators.get(0).getLogicalNode());
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

  @Test
  public void testParallelPartitioning() {

    DAG dag = new DAG();

    GenericTestModule o1 = dag.addOperator("o1", GenericTestModule.class);

    GenericTestModule o2 = dag.addOperator("o2", GenericTestModule.class);
    dag.getOperatorWrapper(o2).getAttributes().attr(OperatorContext.INITIAL_PARTITION_COUNT).set(2);

    GenericTestModule o3 = dag.addOperator("o3", GenericTestModule.class);

    dag.addStream("o1Output1", o1.outport1, o2.inport1, o3.inport1).setInline(true);

    dag.addStream("o2Output1", o2.outport1, o3.inport2).setInline(false);
    dag.setInputPortAttribute(o3.inport2, PortContext.PARTITION_PARALLEL, true);

    // fork parallel partition to two downstream operators
    GenericTestModule o3_1 = dag.addOperator("o3_1", GenericTestModule.class);
    dag.setInputPortAttribute(o3_1.inport1, PortContext.PARTITION_PARALLEL, true);
    OperatorMeta o3_1Meta = dag.getOperatorWrapper(o3_1);

    GenericTestModule o3_2 = dag.addOperator("o3_2", GenericTestModule.class);
    dag.setInputPortAttribute(o3_2.inport1, PortContext.PARTITION_PARALLEL, true);
    OperatorMeta o3_2Meta = dag.getOperatorWrapper(o3_2);

    dag.addStream("o3outport1", o3.outport1, o3_1.inport1, o3_2.inport1).setInline(false); // inline implicit

    // join within parallel partitions
    GenericTestModule o4 = dag.addOperator("o4", GenericTestModule.class);
    dag.setInputPortAttribute(o4.inport1, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(o4.inport2, PortContext.PARTITION_PARALLEL, true);
    OperatorMeta o4Meta = dag.getOperatorWrapper(o4);

    dag.addStream("o3_1.outport1", o3_1.outport1, o4.inport1).setInline(false); // inline implicit
    dag.addStream("o3_2.outport1", o3_2.outport1, o4.inport2).setInline(false); // inline implicit

    // non inline
    GenericTestModule o5merge = dag.addOperator("o5merge", GenericTestModule.class);
    dag.addStream("o4outport1", o4.outport1, o5merge.inport1);

    int maxContainers = 5;
    dag.setAttribute(DAG.STRAM_MAX_CONTAINERS, maxContainers);

    PhysicalPlan plan = new PhysicalPlan(dag, null);
    Assert.assertEquals("number of containers", 5, plan.getContainers().size());

    PTContainer container1 = plan.getContainers().get(0);
    Assert.assertEquals("number operators " + container1, 1, container1.operators.size());
    Assert.assertEquals("operators " + container1, "o1", container1.operators.get(0).getLogicalId());

    for (int i = 1; i < 3; i++) {
      PTContainer container2 = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container2, 5, container2.operators.size());
      Set<String> expectedLogicalNames = Sets.newHashSet("o2", "o3", o3_1Meta.getId(), o3_2Meta.getId(), o4Meta.getId());
      Set<String> actualLogicalNames = Sets.newHashSet();
      for (PTOperator p: container2.operators) {
        actualLogicalNames.add(p.getLogicalId());
      }
      Assert.assertEquals("operator names " + container2, expectedLogicalNames, actualLogicalNames);
    }

    List<OperatorMeta> inlineOperators = Lists.newArrayList(dag.getOperatorWrapper(o2), o3_1Meta, o3_2Meta);
    for (OperatorMeta ow: inlineOperators) {
      List<PTOperator> partitionedInstances = plan.getOperators(ow);
      Assert.assertEquals("" + partitionedInstances, 2, partitionedInstances.size());
      for (PTOperator p: partitionedInstances) {
        Assert.assertEquals("outputs " + p, 1, p.outputs.size());
        Assert.assertTrue("downstream inline " + p.outputs.get(0), p.outputs.get(0).isDownStreamInline());
      }
    }

    // container 4: merge operator for o4
    Map<DAG.OutputPortMeta, PTOperator> o4Unifiers = plan.getMergeOperators(o4Meta);
    Assert.assertEquals("unifier " + o4Meta + ": " + o4Unifiers, 1, o4Unifiers.size());
    PTContainer container4 = plan.getContainers().get(3);
    Assert.assertEquals("number operators " + container4, 1, container4.operators.size());
    Assert.assertEquals("operators " + container4, o4Meta, container4.operators.get(0).logicalNode);
    Assert.assertTrue("unifier " + o4, container4.operators.get(0).merge instanceof Unifier);
    Assert.assertEquals("unifier inputs" + container4.operators.get(0).inputs, 2, container4.operators.get(0).inputs.size());

    // container 5: o5 taking input from o4 unifier
    OperatorMeta o5Meta = dag.getOperatorWrapper(o5merge);
    PTContainer container5 = plan.getContainers().get(4);
    Assert.assertEquals("number operators " + container5, 1, container5.operators.size());
    Assert.assertEquals("operators " + container5, o5Meta, container5.operators.get(0).logicalNode);
    List<PTOperator> o5Instances = plan.getOperators(o5Meta);
    Assert.assertEquals("" + o5Instances, 1, o5Instances.size());
    Assert.assertEquals("inputs" + container5.operators.get(0).inputs, 1, container5.operators.get(0).inputs.size());
    Assert.assertEquals("inputs" + container5.operators.get(0).inputs, container4.operators.get(0), container5.operators.get(0).inputs.get(0).source.source);

  }

  /**
   * When source and sink of a stream are partitioned, a separate unifier is created per downstream instance.
   */
  @Test
  public void testUnifierPartitioning() {

    DAG dag = new DAG();

    TestGeneratorInputModule o1 = dag.addOperator("o1", TestGeneratorInputModule.class);
    dag.setAttribute(o1, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    OperatorMeta o1Meta = dag.getOperatorWrapper(o1);

    GenericTestModule o2 = dag.addOperator("o2", GenericTestModule.class);
    dag.setAttribute(o2, OperatorContext.INITIAL_PARTITION_COUNT, 3);
    OperatorMeta o2Meta = dag.getOperatorWrapper(o2);

    dag.addStream("o1.outport1", o1.outport, o2.inport1);

    int maxContainers = 10;
    dag.setAttribute(DAG.STRAM_MAX_CONTAINERS, maxContainers);

    PhysicalPlan plan = new PhysicalPlan(dag, null);
    Assert.assertEquals("number of containers", 5, plan.getContainers().size());

    List<PTOperator> inputOperators = new ArrayList<PTOperator>();
    for (int i=0; i<2; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 1, container.operators.size());
      Assert.assertEquals("operators " + container, o1Meta.getId(), container.operators.get(0).getLogicalId());
      inputOperators.add(container.operators.get(0));
    }

    for (int i=2; i<5; i++) {
      PTContainer container = plan.getContainers().get(i);
      Assert.assertEquals("number operators " + container, 2, container.operators.size());
      Assert.assertEquals("operators " + container, o2Meta.getId(), container.operators.get(0).getLogicalId());
      Set<String> expectedLogicalNames = Sets.newHashSet(o1Meta.getId(), o2Meta.getId());
      Map<String, PTOperator> actualOperators = new HashMap<String, PTOperator>();
      for (PTOperator p : container.operators) {
        actualOperators.put(p.getLogicalId(), p);
      }
      Assert.assertEquals("", expectedLogicalNames, actualOperators.keySet());

      PTOperator pUnifier = actualOperators.get(o1Meta.getId());
      Assert.assertNotNull("" + pUnifier, pUnifier.container);
      Assert.assertTrue("" + pUnifier, pUnifier.merge instanceof Unifier);
      // input from each upstream partition
      Assert.assertEquals("" + pUnifier, 2, pUnifier.inputs.size());
      for (int inputIndex = 0; i < pUnifier.inputs.size(); i++) {
        PTInput input = pUnifier.inputs.get(inputIndex);
        Assert.assertEquals("" + pUnifier, "outputPort", input.source.portName);
        Assert.assertEquals("" + pUnifier, inputOperators.get(inputIndex), input.source.source);
        Assert.assertEquals("partition keys " + input.partitions, 1, input.partitions.partitions.size());
      }
      // output to single downstream partition
      Assert.assertEquals("" + pUnifier, 1, pUnifier.outputs.size());
      Assert.assertTrue(""+actualOperators.get(o2Meta.getId()).logicalNode.getOperator(), actualOperators.get(o2Meta.getId()).logicalNode.getOperator() instanceof GenericTestModule);

      PTOperator p = actualOperators.get(o2Meta.getId());
      Assert.assertEquals("partition inputs " + p.inputs, 1, p.inputs.size());
      Assert.assertEquals("partition inputs " + p.inputs, pUnifier, p.inputs.get(0).source.source);
    }

  }


}

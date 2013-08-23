package com.datatorrent.stram;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.FutureTask;

import javax.validation.ValidationException;

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.api.DAG.StreamMeta;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.PhysicalPlan.PTContainer;
import com.datatorrent.stram.PhysicalPlan.PTInput;
import com.datatorrent.stram.PhysicalPlan.PTOperator;
import com.datatorrent.stram.PhysicalPlanTest.TestPlanContext;
import com.datatorrent.stram.plan.logical.CreateOperatorRequest;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanRequest;
import com.datatorrent.stram.plan.physical.PlanModifier;
import com.google.common.collect.Sets;

public class LogicalPlanModificationTest {

  @Test
  public void testAddOperator()
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    ctx.deploy.clear();
    ctx.undeploy.clear();

    Assert.assertEquals("containers", 3, plan.getContainers().size());

    PlanModifier pm = new PlanModifier(plan);
    GenericTestOperator added1 = new GenericTestOperator();
    pm.addOperator("added1", added1);

    pm.addStream("added1.outport1", added1.outport1, o3.inport2);

    Assert.assertEquals("undeploy " + ctx.undeploy, 0, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 0, ctx.deploy.size());

    pm.applyChanges(ctx);

    Assert.assertEquals("containers post change", 4, plan.getContainers().size());

    Assert.assertEquals("undeploy " + ctx.undeploy, 1, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 2, ctx.deploy.size());

  }

  @Test
  public void testSetOperatorProperty()
  {
    LogicalPlan dag = new LogicalPlan();
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    OperatorMeta o1Meta = dag.getMeta(o1);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    ctx.deploy.clear();
    ctx.undeploy.clear();

    PlanModifier pm = new PlanModifier(plan);
    try {
      pm.setOperatorProperty(o1Meta.getName(), "myStringProperty", "propertyValue");
      Assert.fail("validation error exepected");
    } catch (javax.validation.ValidationException e) {
      Assert.assertTrue(e.getMessage().contains(o1Meta.toString()));
    }

    GenericTestOperator newOperator = new GenericTestOperator();
    pm.addOperator("newOperator", newOperator);
    pm.setOperatorProperty("newOperator", "myStringProperty", "propertyValue");
    Assert.assertEquals("", "propertyValue", newOperator.getMyStringProperty());
  }

  @Test
  public void testRemoveOperator()
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    OperatorMeta o1Meta = dag.getMeta(o1);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    OperatorMeta o2Meta = dag.getMeta(o2);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    OperatorMeta o3Meta = dag.getMeta(o3);

    StreamMeta s1 = dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    StreamMeta s2 = dag.addStream("o2.outport1", o2.outport1, o3.inport1);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    ctx.deploy.clear();
    ctx.undeploy.clear();
    Assert.assertEquals("containers " + plan.getContainers(), 3, plan.getContainers().size());
    Assert.assertEquals("physical operators " + plan.getAllOperators(), 3, plan.getAllOperators().size());

    PlanModifier pm = new PlanModifier(plan);

    try {
      pm.removeOperator(o2Meta.getName());
      Assert.fail("validation error (connected streams) expected");
    } catch (ValidationException ve) {
    }

    // remove stream required before removing operator
    pm.removeStream(s1.getId());
    pm.removeStream(s2.getId());

    pm.removeOperator(o2Meta.getName());
    pm.applyChanges(ctx);

    Assert.assertEquals("streams " + dag.getAllStreams(), 0, dag.getAllStreams().size());
    Assert.assertEquals("operators " + dag.getAllOperators(), 2, dag.getAllOperators().size());
    Assert.assertTrue("operators " + dag.getAllOperators(), dag.getAllOperators().containsAll(Sets.newHashSet(o1Meta, o3Meta)));

    try {
      plan.getOperators(o2Meta);
      Assert.fail("removed from physical plan: " + o2Meta);
    } catch (Exception e) {
    }
    Assert.assertEquals("containers " + plan.getContainers(), 2, plan.getContainers().size());
    Assert.assertEquals("physical operators " + plan.getAllOperators(), 2, plan.getAllOperators().size());
    Assert.assertEquals("removed containers " + ctx.releaseContainers, 1, ctx.releaseContainers.size());
  }

  @Test
  public void testRemoveStream()
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    PlanModifier pm = new PlanModifier(plan);
    pm.removeStream("o1.outport1");
    pm.applyChanges(ctx);

    Assert.assertEquals("undeploy " + ctx.undeploy, 2, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 2, ctx.deploy.size());
  }

  @Test
  public void testAddStream()
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    List<PTOperator> o1Instances = plan.getOperators(dag.getMeta(o1));
    Assert.assertEquals("o1Instances " + o1Instances, 1, o1Instances.size());
    PTOperator o1p1 = o1Instances.get(0);

    OperatorMeta om2 = dag.getMeta(o2);
    List<PTOperator> o2Instances = plan.getOperators(om2);
    Assert.assertEquals("o2Instances " + o2Instances, 1, o2Instances.size());
    PTOperator o2p1 = o2Instances.get(0);

    Assert.assertEquals("outputs " + o1p1, 0, o1p1.outputs.size());
    Assert.assertEquals("inputs " + o2p1, 0, o2p1.inputs.size());

    PlanModifier pm = new PlanModifier(plan);
    pm.addStream("o1.outport1", o1.outport1, o2.inport1);
    pm.addStream("o1.outport1", o1.outport1, o2.inport2);
    pm.applyChanges(ctx);

    Assert.assertEquals("undeploy " + ctx.undeploy, 2, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 2, ctx.deploy.size());

    Assert.assertEquals("outputs " + o1p1, 1, o1p1.outputs.size());
    Assert.assertEquals("inputs " + o2p1, 2, o2p1.inputs.size());
    Set<String> portNames = Sets.newHashSet();
    for (PTInput in : o2p1.inputs) {
      portNames.add(in.portName);
    }
    Set<String> expPortNames = Sets.newHashSet(GenericTestOperator.IPORT1, GenericTestOperator.IPORT2);
    Assert.assertEquals("input port names " + o2p1.inputs, expPortNames, portNames);

  }

  @Test
  public void testExecutionManager() throws Exception {

    LogicalPlan dag = new LogicalPlan();
    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    Assert.assertEquals(""+dnm.containerStartRequests, dnm.containerStartRequests.size(), 0);


    CreateOperatorRequest cor = new CreateOperatorRequest();
    cor.setOperatorFQCN(GenericTestOperator.class.getName());
    cor.setOperatorName("o1");

    FutureTask<?> lpmf = dnm.logicalPlanModification(Collections.<LogicalPlanRequest>singletonList(cor));
    while (!lpmf.isDone()) {
      dnm.processEvents();
    }

    lpmf.get();

    Assert.assertEquals(""+dnm.containerStartRequests, 1, dnm.containerStartRequests.size());
    PTContainer c = dnm.containerStartRequests.poll().container;
    Assert.assertEquals("operators "+c, 1, c.operators.size());

    Assert.assertEquals("deploy requests " + c, 1, c.pendingDeploy.size());

    PTOperator oper = c.operators.get(0);
    Assert.assertEquals("operator name", "o1", oper.getOperatorMeta().getName());
    Assert.assertEquals("operator class", GenericTestOperator.class, oper.getOperatorMeta().getOperator().getClass());

  }

}

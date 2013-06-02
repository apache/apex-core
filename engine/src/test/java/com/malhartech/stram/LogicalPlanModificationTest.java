package com.malhartech.stram;

import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.malhartech.engine.GenericTestOperator;
import com.malhartech.stram.PhysicalPlan.PTInput;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.PhysicalPlanTest.TestPlanContext;
import com.malhartech.stram.plan.logical.LogicalPlan;
import com.malhartech.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.malhartech.stram.plan.logical.LogicalPlan.StreamMeta;
import com.malhartech.stram.plan.physical.PlanModifier;

public class LogicalPlanModificationTest {

  @Ignore
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

    PlanModifier pm = new PlanModifier(plan, ctx);
    GenericTestOperator added1 = new GenericTestOperator();
    pm.addOperator("added1", added1);

    @SuppressWarnings("unchecked")
    StreamMeta sm = pm.addStream("added1.outport1", added1.outport1, o3.inport2);

    pm.applyChanges();

    Assert.assertEquals("undeploy " + ctx.undeploy, 1, ctx.undeploy.size());
    Assert.assertEquals("deploy " + ctx.deploy, 2, ctx.deploy.size());

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

    PlanModifier pm = new PlanModifier(plan, ctx);
    pm.removeStream("o1.outport1");
    pm.applyChanges();

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

    PlanModifier pm = new PlanModifier(plan, ctx);
    pm.addStream("o1.outport1", o1.outport1, o2.inport1);
    pm.addStream("o1.outport1", o1.outport1, o2.inport2);
    pm.applyChanges();

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

}

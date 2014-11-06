package com.datatorrent.stram;

import org.junit.Assert;

import org.junit.Test;

import com.datatorrent.api.StringCodec;
import com.datatorrent.stram.engine.GenericOperatorProperty;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.TestPlanContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.plan.physical.PlanModifier;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;
import java.util.*;

public class GenericOperatorPropertyCodecTest
{

  @Test
  public void testGenericOperatorPropertyCodec()
  {
    LogicalPlan dag = new LogicalPlan();
    Map<Class<?>, Class<? extends StringCodec<?>>> codecs = new HashMap<Class<?>, Class<? extends StringCodec<?>>>();
    codecs.put(GenericOperatorProperty.class, GenericOperatorProperty.GenericOperatorPropertyStringCodec.class);
    dag.setAttribute(com.datatorrent.api.Context.DAGContext.STRING_CODECS, codecs);
    dag.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    OperatorMeta o1Meta = dag.getMeta(o1);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);
    ctx.deploy.clear();
    ctx.undeploy.clear();

    PlanModifier pm = new PlanModifier(plan);
    try {
      pm.setOperatorProperty(o1Meta.getName(), "genericOperatorProperty", "xyz");
      Assert.fail("validation error expected"); // cannot set properties on an operator that is already deployed.
    } catch (javax.validation.ValidationException e) {
      Assert.assertTrue(e.getMessage().contains(o1Meta.toString()));
    }

    GenericTestOperator newOperator = new GenericTestOperator();
    pm.addOperator("newOperator", newOperator);
    pm.setOperatorProperty("newOperator", "genericOperatorProperty", "xyz");
    Assert.assertEquals("", "xyz", newOperator.getGenericOperatorProperty().obtainString());
  }
}

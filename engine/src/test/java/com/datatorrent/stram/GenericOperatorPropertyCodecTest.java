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
package com.datatorrent.stram;

import java.util.HashMap;
import java.util.Map;

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

public class GenericOperatorPropertyCodecTest
{

  @Test
  public void testGenericOperatorPropertyCodec()
  {
    LogicalPlan dag = new LogicalPlan();
    Map<Class<?>, Class<? extends StringCodec<?>>> codecs = new HashMap<>();
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

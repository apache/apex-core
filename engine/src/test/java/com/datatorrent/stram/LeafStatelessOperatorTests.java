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

import java.net.InetSocketAddress;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.StramTestSupport;

import static com.datatorrent.stram.StreamingContainerManagerTest.getNodeDeployInfo;

public class LeafStatelessOperatorTests
{

  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  private LogicalPlan dag;

  @Before
  public void setup()
  {
    dag = StramTestSupport.createDAG(testMeta);
  }

  @Stateless
  public static class StatelessOperator extends GenericTestOperator
  {
  }

  @Stateless
  public static class StatelessInputOperator extends TestGeneratorInputOperator
  {

  }

  /**
   * Test operator with explicit stateless attribute set.
   */
  @Test
  public void testGenerateDeployInfo()
  {
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    dag.setOperatorAttribute(o3, Context.OperatorContext.STATELESS, true);
    GenericTestOperator o4 = dag.addOperator("o4", GenericTestOperator.class);
    dag.setOperatorAttribute(o4, Context.OperatorContext.STATELESS, true);

    dag.addStream("o1.outport", o1.outport, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("o3.outport1", o3.outport1, o4.inport1).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    Assert.assertEquals("number of leaf operators ", 1, dag.getLeafOperators().size());

    verifyStatelessAttribute(Lists.newArrayList(null, null, true, false));
  }

  /**
   * Test operator with stateless annotation.
   */
  @Test
  public void testGenerateDeployInfo1()
  {
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    StatelessOperator o3 = dag.addOperator("o3", StatelessOperator.class);
    StatelessOperator o4 = dag.addOperator("o4", StatelessOperator.class);

    dag.addStream("o1.outport", o1.outport, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("o3.outport1", o3.outport1, o4.inport1).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    Assert.assertEquals("number of leaf operators ", 1, dag.getLeafOperators().size());

    verifyStatelessAttribute(Lists.newArrayList(null, null, true, false));
  }

  @Test
  public void testAllStatelessOperator()
  {
    StatelessInputOperator o1 = dag.addOperator("o1", StatelessInputOperator.class);
    StatelessOperator o2 = dag.addOperator("o2", StatelessOperator.class);
    StatelessOperator o3 = dag.addOperator("o3", StatelessOperator.class);
    StatelessOperator o4 = dag.addOperator("o4", StatelessOperator.class);

    dag.addStream("o1.outport", o1.outport, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("o3.outport1", o3.outport1, o4.inport1).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    Assert.assertEquals("number of leaf operators ", 1, dag.getLeafOperators().size());

    // all except last operator is stateless.
    verifyStatelessAttribute(Lists.newArrayList(true, true, true, false));
  }

  @Test
  public void testWithDAGStatelessAttribute()
  {
    dag.setAttribute(Context.OperatorContext.STATELESS, true);
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    GenericTestOperator o4 = dag.addOperator("o4", GenericTestOperator.class);

    dag.addStream("o1.outport", o1.outport, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("o3.outport1", o3.outport1, o4.inport1).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    Assert.assertEquals("number of leaf operators ", 1, dag.getLeafOperators().size());

    verifyStatelessAttribute(Lists.newArrayList(true, true, true, false));
  }


  private void verifyStatelessAttribute(List<Boolean> values)
  {
    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    dnm.assignContainer(new StreamingContainerManager.ContainerResource(0, "container1Id", "host1", 1024, 0, null), InetSocketAddress.createUnresolved("host1", 9001));
    StreamingContainerAgent sca1 = dnm.getContainerAgent(dnm.getPhysicalPlan().getContainers().get(0).getExternalId());
    List<OperatorDeployInfo> c1 = sca1.getDeployInfoList(sca1.container.getOperators());

    OperatorDeployInfo odi = getNodeDeployInfo(c1, dag.getOperatorMeta("o1"));
    Assert.assertEquals("o1 is stateful operator ", odi.contextAttributes.get(Context.OperatorContext.STATELESS), values.get(0));

    odi = getNodeDeployInfo(c1, dag.getOperatorMeta("o2"));
    Assert.assertEquals("o2 is stateful operator ", odi.contextAttributes.get(Context.OperatorContext.STATELESS), values.get(1));

    odi = getNodeDeployInfo(c1, dag.getOperatorMeta("o3"));
    Assert.assertEquals("o3 is stateful operator ", odi.contextAttributes.get(Context.OperatorContext.STATELESS), values.get(2));

    odi = getNodeDeployInfo(c1, dag.getOperatorMeta("o4"));
    Assert.assertEquals("o4 is stateful operator ", odi.contextAttributes.get(Context.OperatorContext.STATELESS), values.get(3));
  }

}

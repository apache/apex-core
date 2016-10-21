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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestOutputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.stream.OiOEndWindowTest.TestInputOperator;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;

/**
 *
 */
public class OutputUnifiedTest
{

  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  private LogicalPlan dag;

  @Before
  public void setup()
  {
    dag = StramTestSupport.createDAG(testMeta);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());
  }

  @Test
  public void testManyToOnePartition() throws Exception
  {
    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    GenericTestOperator op1 = new GenericTestOperator();
    dag.addOperator("op1", op1);

    dag.setOperatorAttribute(op1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));

    TestOutputOperator op2 = new TestOutputOperator();
    dag.addOperator("op2", op2);

    dag.addStream("s1", i1.output, op1.inport1);
    dag.addStream("s2", op1.outport1, op2.inport);

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan physicalPlan = scm.getPhysicalPlan();
    List<PTContainer> containers = physicalPlan.getContainers();
    Assert.assertEquals("Number of containers", 5, containers.size());

    assignContainers(scm, containers);

    testOutputAttribute(dag, i1, scm, physicalPlan, false);
    testOutputAttribute(dag, op1, scm, physicalPlan, true);

  }

  @Test
  public void testMxNPartition() throws Exception
  {
    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    GenericTestOperator op1 = new GenericTestOperator();
    dag.addOperator("op1", op1);

    dag.setOperatorAttribute(op1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));

    TestOutputOperator op2 = new TestOutputOperator();
    dag.addOperator("op2", op2);

    dag.setOperatorAttribute(op2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));

    dag.addStream("s1", i1.output, op1.inport1);
    dag.addStream("s2", op1.outport1, op2.inport);

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan physicalPlan = scm.getPhysicalPlan();
    List<PTContainer> containers = physicalPlan.getContainers();
    Assert.assertEquals("Number of containers", 6, containers.size());

    assignContainers(scm, containers);

    testOutputAttribute(dag, i1, scm, physicalPlan, false);
    testOutputAttribute(dag, op1, scm, physicalPlan, true);

  }

  @Test
  public void testParallelPartition() throws Exception
  {
    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    dag.setOperatorAttribute(i1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));

    GenericTestOperator op1 = new GenericTestOperator();
    dag.addOperator("op1", op1);

    dag.setInputPortAttribute(op1.inport1, PortContext.PARTITION_PARALLEL, true);

    TestOutputOperator op2 = new TestOutputOperator();
    dag.addOperator("op2", op2);

    dag.addStream("s1", i1.output, op1.inport1);
    dag.addStream("s2", op1.outport1, op2.inport);

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan physicalPlan = scm.getPhysicalPlan();
    List<PTContainer> containers = physicalPlan.getContainers();
    Assert.assertEquals("Number of containers", 5, containers.size());

    assignContainers(scm, containers);

    testOutputAttribute(dag, i1, scm, physicalPlan, false);
    testOutputAttribute(dag, op1, scm, physicalPlan, true);
  }

  private void testOutputAttribute(LogicalPlan dag, Operator operator, StreamingContainerManager scm, PhysicalPlan physicalPlan, boolean result)
  {
    List<PTOperator> ptOperators = physicalPlan.getOperators(dag.getMeta(operator));
    for (PTOperator ptOperator : ptOperators) {
      PTContainer container = ptOperator.getContainer();
      StreamingContainerAgent agent = scm.getContainerAgent("container" + container.getId());
      List<OperatorDeployInfo> deployInfoList = agent.getDeployInfoList(container.getOperators());
      Assert.assertEquals("Deploy info size", 1, deployInfoList.size());
      Assert.assertEquals("Is output unified", deployInfoList.get(0).outputs.get(0).getAttributes().get(PortContext.IS_OUTPUT_UNIFIED), result);
    }
  }

  private void assignContainers(StreamingContainerManager scm, List<PTContainer> containers)
  {
    for (PTContainer container : containers) {
      assignContainer(scm, "container" + container.getId());
    }
  }

  private static StreamingContainerAgent assignContainer(StreamingContainerManager scm, String containerId)
  {
    return scm.assignContainer(new StreamingContainerManager.ContainerResource(0, containerId, "localhost", 1024, 0, null), InetSocketAddress.createUnresolved(containerId + "Host", 0));
  }

}

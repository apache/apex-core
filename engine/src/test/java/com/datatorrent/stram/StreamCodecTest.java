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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.StramTestSupport;

/**
 */
public class StreamCodecTest
{
  private LogicalPlan dag;

  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  @Before
  public void setup()
  {
    dag = StramTestSupport.createDAG(testMeta);
  }

  @Test
  public void testStreamCodec()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, new TestStreamCodec());

    dag.addStream("n1n2", node1.outport1, node2.inport1);
    dag.addStream("n2n3", node2.outport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 3, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    OperatorDeployInfo n1di = getSingleOperatorDeployInfo(node1, dnm);

    OperatorDeployInfo.OutputDeployInfo n1odi = getOutputDeployInfo(n1di, n1meta.getMeta(node1.outport1));
    String id = n1meta.getName() + " " + n1odi.portName;
    Assert.assertEquals("number stream codecs " + id, n1odi.streamCodecs.size(), 1);
    Assert.assertTrue("No user set stream codec", n1odi.streamCodecs.containsValue(null));

    OperatorDeployInfo n2di = getSingleOperatorDeployInfo(node2, dnm);

    OperatorDeployInfo.InputDeployInfo n2idi = getInputDeployInfo(n2di, n2meta.getMeta(node2.inport1));
    id = n2meta.getName() + " " + n2idi.portName;
    Assert.assertEquals("number stream codecs " + id, n2idi.streamCodecs.size(), 1);
    Assert.assertTrue("No user set stream codec", n2idi.streamCodecs.containsValue(null));

    OperatorDeployInfo.OutputDeployInfo n2odi = getOutputDeployInfo(n2di, n2meta.getMeta(node2.outport1));
    id = n2meta.getName() + " " + n2odi.portName;
    Assert.assertEquals("number stream codecs " + id, n2odi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n3meta, node3.inport1, n2odi.streamCodecs, id, plan);


    OperatorDeployInfo n3di = getSingleOperatorDeployInfo(node3, dnm);

    OperatorDeployInfo.InputDeployInfo n3idi = getInputDeployInfo(n3di, n3meta.getMeta(node3.inport1));
    id = n3meta.getName() + " " + n3idi.portName;
    Assert.assertEquals("number stream codecs " + id, n3idi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n3meta, node3.inport1, n3idi.streamCodecs, id, plan);
  }

  @Test
  public void testStreamCodecReuse()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    GenericTestOperator node4 = dag.addOperator("node4", GenericTestOperator.class);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node4.inport1, Context.PortContext.STREAM_CODEC, serDe);
    GenericTestOperator node5 = dag.addOperator("node5", GenericTestOperator.class);
    dag.setInputPortAttribute(node5.inport1, Context.PortContext.STREAM_CODEC, serDe);
    GenericTestOperator node6 = dag.addOperator("node6", GenericTestOperator.class);
    serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node6.inport1, Context.PortContext.STREAM_CODEC, serDe);

    dag.addStream("n1n2", node1.outport1, node2.inport1);
    dag.addStream("n2n3", node2.outport1, node3.inport1);
    dag.addStream("n3n4", node3.outport1, node4.inport1);
    dag.addStream("n4n5", node4.outport1, node5.inport1);
    dag.addStream("n5n6", node5.outport1, node6.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 6, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    getSingleOperatorDeployInfo(node1, dnm);
    getSingleOperatorDeployInfo(node2, dnm);
    getSingleOperatorDeployInfo(node3, dnm);
    getSingleOperatorDeployInfo(node4, dnm);
    getSingleOperatorDeployInfo(node5, dnm);
    getSingleOperatorDeployInfo(node6, dnm);
    Assert.assertEquals("number of stream codec identifiers", 3, plan.getStreamCodecIdentifiers().size());
  }

  @Test
  public void testDefaultStreamCodec()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    DefaultCodecOperator node2 = dag.addOperator("node2", DefaultCodecOperator.class);
    DefaultCodecOperator node3 = dag.addOperator("node3", DefaultCodecOperator.class);
    dag.setInputPortAttribute(node3.inportWithCodec, Context.PortContext.STREAM_CODEC, new TestStreamCodec());

    dag.addStream("n1n2", node1.outport1, node2.inportWithCodec);
    dag.addStream("n2n3", node2.outport1, node3.inportWithCodec);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 3, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    OperatorDeployInfo n1di = getSingleOperatorDeployInfo(node1, dnm);

    OperatorDeployInfo.OutputDeployInfo n1odi = getOutputDeployInfo(n1di, n1meta.getMeta(node1.outport1));
    String id = n1meta.getName() + " " + n1odi.portName;
    Assert.assertEquals("number stream codecs " + id, n1odi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n2meta, node2.inportWithCodec, n1odi.streamCodecs, id, plan);

    OperatorDeployInfo n2di = getSingleOperatorDeployInfo(node2, dnm);

    OperatorDeployInfo.InputDeployInfo n2idi = getInputDeployInfo(n2di, n2meta.getMeta(node2.inportWithCodec));
    id = n2meta.getName() + " " + n2idi.portName;
    Assert.assertEquals("number stream codecs " + id, n2idi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n2meta, node2.inportWithCodec, n2idi.streamCodecs, id, plan);

    OperatorDeployInfo.OutputDeployInfo n2odi = getOutputDeployInfo(n2di, n2meta.getMeta(node2.outport1));
    id = n2meta.getName() + " " + n2odi.portName;
    Assert.assertEquals("number stream codecs " + id, n2odi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n3meta, node3.inportWithCodec, n2odi.streamCodecs, id, plan);

    OperatorDeployInfo n3di = getSingleOperatorDeployInfo(node3, dnm);

    OperatorDeployInfo.InputDeployInfo n3idi = getInputDeployInfo(n3di, n3meta.getMeta(node3.inportWithCodec));
    id = n3meta.getName() + " " + n3idi.portName;
    Assert.assertEquals("number stream codecs " + id, n3idi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n3meta, node3.inportWithCodec, n3idi.streamCodecs, id, plan);
  }

  @Test
  public void testPartitioningStreamCodec()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setOperatorAttribute(node2, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);

    dag.addStream("n1n2", node1.outport1, node2.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 4, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);

    OperatorDeployInfo n1di = getSingleOperatorDeployInfo(node1, dnm);

    OperatorDeployInfo.OutputDeployInfo n1odi = getOutputDeployInfo(n1di, n1meta.getMeta(node1.outport1));
    String id = n1meta.getName() + " " + n1odi.portName;
    Assert.assertEquals("number stream codecs " + id, n1odi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n2meta, node2.inport1, n1odi.streamCodecs, id, plan);


    List<PTOperator> operators = plan.getOperators(n2meta);
    Assert.assertEquals("number operators " + n2meta.getName(), 3, operators.size());
    for (PTOperator operator : operators) {
      OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

      OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
      id = n2meta.getName() + " " + idi.portName;
      Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
      checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);
    }
  }

  @Test
  public void testMxNPartitioningStreamCodec()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    dag.setOperatorAttribute(node1, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setOperatorAttribute(node2, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);

    dag.addStream("n1n2", node1.outport1, node2.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);

    // Sanity check that physical operators have been allocated for n1meta and n2meta
    Assert.assertEquals("number operators " + n1meta.getName(), 2, plan.getOperators(n1meta).size());
    Assert.assertEquals("number operators " + n2meta.getName(), 3, plan.getOperators(n2meta).size());

    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator : operators) {
        if (!operator.isUnifier()) {
          if (operator.getOperatorMeta() == n1meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n1meta.getName(), dnm);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n1meta.getMeta(node1.outport1));
            String id = n1meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, otdi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n2meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
            String id = n1meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);
          }
        } else {
          OperatorDeployInfo odi = getOperatorDeployInfo(operator, operator.getName(), dnm);
          List<OperatorDeployInfo.InputDeployInfo> idis = odi.inputs;
          for (OperatorDeployInfo.InputDeployInfo idi : idis) {
            String id = operator.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);
          }
        }
      }
    }
  }

  @Test
  public void testParallelPartitioningStreamCodec()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    dag.setOperatorAttribute(node1, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.PARTITION_PARALLEL, true);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    TestStreamCodec2 serDe2 = new TestStreamCodec2();
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe2);

    dag.addStream("n1n2", node1.outport1, node2.inport1);
    dag.addStream("n2n3", node2.outport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    // Sanity check that physical operators have been allocated for n1meta and n2meta
    Assert.assertEquals("number operators " + n1meta.getName(), 2, plan.getOperators(n1meta).size());
    Assert.assertEquals("number operators " + n2meta.getName(), 2, plan.getOperators(n2meta).size());
    Assert.assertEquals("number operators " + n3meta.getName(), 1, plan.getOperators(n3meta).size());

    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator :operators) {
        if (!operator.isUnifier()) {
          if (operator.getOperatorMeta() == n1meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n1meta.getName(), dnm);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n1meta.getMeta(node1.outport1));
            String id = n1meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, otdi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n2meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
            String id = n1meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n2meta.getMeta(node2.outport1));
            id = n2meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n3meta, node3.inport1, otdi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n3meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n3meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n3meta.getMeta(node3.inport1));
            String id = n3meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n3meta, node3.inport1, idi.streamCodecs, id, plan);
          }
        } else {
          OperatorDeployInfo odi = getOperatorDeployInfo(operator, operator.getName(), dnm);
          List<OperatorDeployInfo.InputDeployInfo> idis = odi.inputs;
          for (OperatorDeployInfo.InputDeployInfo idi : idis) {
            String id = operator.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n3meta, node3.inport1, idi.streamCodecs, id, plan);
          }
          List<OperatorDeployInfo.OutputDeployInfo> otdis = odi.outputs;
          for (OperatorDeployInfo.OutputDeployInfo otdi : otdis) {
            String id = operator.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 0);
          }
        }
      }
    }
  }

  @Test
  public void testMultipleInputStreamCodec()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    TestStreamCodec serDe = new TestStreamCodec();
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe);

    dag.addStream("n1n2n3", node1.outport1, node2.inport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 3, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    OperatorDeployInfo n1di = getSingleOperatorDeployInfo(node1, dnm);

    OperatorDeployInfo.OutputDeployInfo n1odi = getOutputDeployInfo(n1di, n1meta.getMeta(node1.outport1));
    String id = n1meta.getName() + " " + n1odi.portName;
    Assert.assertEquals("number stream codecs " + id, n1odi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n2meta, node2.inport1, n1odi.streamCodecs, id, plan);

    OperatorDeployInfo n2di = getSingleOperatorDeployInfo(node2, dnm);

    OperatorDeployInfo.InputDeployInfo n2idi = getInputDeployInfo(n2di, n2meta.getMeta(node2.inport1));
    id = n2meta.getName() + " " + n2idi.portName;
    Assert.assertEquals("number stream codecs " + id, n2idi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n2meta, node2.inport1, n2idi.streamCodecs, id, plan);

    OperatorDeployInfo n3di = getSingleOperatorDeployInfo(node3, dnm);

    OperatorDeployInfo.InputDeployInfo n3idi = getInputDeployInfo(n3di, n3meta.getMeta(node3.inport1));
    id = n3meta.getName() + " " + n3idi.portName;
    Assert.assertEquals("number stream codecs " + id, n3idi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n3meta, node3.inport1, n3idi.streamCodecs, id, plan);
  }

  @Test
  public void testPartitioningMultipleInputStreamCodec()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setOperatorAttribute(node1, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe);

    dag.addStream("n1n2n3", node1.outport1, node2.inport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 4, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator : operators) {
        if (!operator.isUnifier()) {
          if (operator.getOperatorMeta() == n1meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n1meta.getName(), dnm);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n1meta.getMeta(node1.outport1));
            String id = n1meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, otdi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n2meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
            String id = n2meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n3meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n3meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n3meta.getMeta(node3.inport1));
            String id = n3meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n3meta, node3.inport1, idi.streamCodecs, id, plan);
          }
        } else {
          OperatorDeployInfo odi = getOperatorDeployInfo(operator, operator.getName(), dnm);
          List<OperatorDeployInfo.InputDeployInfo> idis = odi.inputs;
          for (OperatorDeployInfo.InputDeployInfo idi : idis) {
            String id = operator.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);
            checkPresentStreamCodec(n3meta, node3.inport1, idi.streamCodecs, id, plan);
          }
          List<OperatorDeployInfo.OutputDeployInfo> otdis = odi.outputs;
          for (OperatorDeployInfo.OutputDeployInfo otdi : otdis) {
            String id = operator.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 0);
          }
        }
      }
    }
  }

  @Test
  public void testMultipleStreamCodecs()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    TestStreamCodec2 serDe2 = new TestStreamCodec2();
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe2);

    dag.addStream("n1n2n3", node1.outport1, node2.inport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 3, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    OperatorDeployInfo n1di = getSingleOperatorDeployInfo(node1, dnm);

    OperatorDeployInfo.OutputDeployInfo n1odi = getOutputDeployInfo(n1di, n1meta.getMeta(node1.outport1));
    String id = n1meta.getName() + " " + n1odi.portName;
    Assert.assertEquals("number stream codecs " + id, n1odi.streamCodecs.size(), 2);
    checkPresentStreamCodec(n2meta, node2.inport1, n1odi.streamCodecs, id, plan);
    checkPresentStreamCodec(n3meta, node3.inport1, n1odi.streamCodecs, id, plan);

    OperatorDeployInfo n2di = getSingleOperatorDeployInfo(node2, dnm);

    OperatorDeployInfo.InputDeployInfo n2idi = getInputDeployInfo(n2di, n2meta.getMeta(node2.inport1));
    id = n2meta.getName() + " " + n2idi.portName;
    Assert.assertEquals("number stream codecs " + id, n2idi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n2meta, node2.inport1, n2idi.streamCodecs, id, plan);

    OperatorDeployInfo n3di = getSingleOperatorDeployInfo(node3, dnm);

    OperatorDeployInfo.InputDeployInfo n3idi = getInputDeployInfo(n3di, n3meta.getMeta(node3.inport1));
    id = n3meta.getName() + " " + n3idi.portName;
    Assert.assertEquals("number stream codecs " + id, n3idi.streamCodecs.size(), 1);
    checkPresentStreamCodec(n3meta, node3.inport1, n3idi.streamCodecs, id, plan);
  }

  @Test
  public void testPartitioningMultipleStreamCodecs()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    dag.setOperatorAttribute(node1, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    TestStreamCodec2 serDe2 = new TestStreamCodec2();
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe2);

    dag.addStream("n1n2n3", node1.outport1, node2.inport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 4, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator : operators) {
        if (!operator.isUnifier()) {
          if (operator.getOperatorMeta() == n1meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n1meta.getName(), dnm);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n1meta.getMeta(node1.outport1));
            String id = n1meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 2);
            checkPresentStreamCodec(n2meta, node2.inport1, otdi.streamCodecs, id, plan);
            checkPresentStreamCodec(n3meta, node3.inport1, otdi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n2meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
            String id = n2meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n3meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n3meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n3meta.getMeta(node3.inport1));
            String id = n3meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n3meta, node3.inport1, idi.streamCodecs, id, plan);
          }
        } else {
          OperatorDeployInfo odi = getOperatorDeployInfo(operator, operator.getName(), dnm);
          Assert.assertEquals("unifier outputs " + operator.getName(), 1, operator.getOutputs().size());
          PTOperator.PTOutput out = operator.getOutputs().get(0);
          Assert.assertEquals("unifier sinks " + operator.getName(), 1, out.sinks.size());
          PTOperator.PTInput idInput = out.sinks.get(0);
          LogicalPlan.OperatorMeta idMeta = idInput.target.getOperatorMeta();
          Operator.InputPort<?> idInputPort = null;
          if (idMeta == n2meta) {
            idInputPort = node2.inport1;
          } else if (idMeta == n3meta) {
            idInputPort = node3.inport1;
          }
          List<OperatorDeployInfo.InputDeployInfo> idis = odi.inputs;
          for (OperatorDeployInfo.InputDeployInfo idi : idis) {
            String id = operator.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(idMeta, idInputPort, idi.streamCodecs, id, plan);
          }
        }
      }
    }
  }

  @Test
  public void testMxNMultipleStreamCodecs()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    dag.setOperatorAttribute(node1, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setOperatorAttribute(node2, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    dag.setOperatorAttribute(node3, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    TestStreamCodec serDe2 = new TestStreamCodec();
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe2);

    dag.addStream("n1n2n3", node1.outport1, node2.inport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    // Sanity check that physical operators have been allocated for n1meta and n2meta
    Assert.assertEquals("number operators " + n1meta.getName(), 2, plan.getOperators(n1meta).size());
    Assert.assertEquals("number operators " + n2meta.getName(), 3, plan.getOperators(n2meta).size());
    Assert.assertEquals("number operators " + n3meta.getName(), 3, plan.getOperators(n3meta).size());

    checkMxNStreamCodecs(node1, node2, node3, dnm);
  }

  private void checkMxNStreamCodecs(GenericTestOperator node1, GenericTestOperator node2, GenericTestOperator node3, StreamingContainerManager dnm)
  {
    LogicalPlan dag = dnm.getLogicalPlan();
    PhysicalPlan plan = dnm.getPhysicalPlan();
    List<PTContainer> containers = plan.getContainers();
    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);
    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator : operators) {
        if (!operator.isUnifier()) {
          if (operator.getOperatorMeta() == n1meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n1meta.getName(), dnm);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n1meta.getMeta(node1.outport1));
            String id = n1meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 2);
            checkPresentStreamCodec(n2meta, node2.inport1, otdi.streamCodecs, id, plan);
            checkPresentStreamCodec(n3meta, node3.inport1, otdi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n2meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
            String id = n2meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n3meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n3meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n3meta.getMeta(node3.inport1));
            String id = n3meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n3meta, node3.inport1, idi.streamCodecs, id, plan);
          }
        } else {
          OperatorDeployInfo odi = getOperatorDeployInfo(operator, operator.getName(), dnm);
          Assert.assertEquals("unifier outputs " + operator.getName(), 1, operator.getOutputs().size());
          PTOperator.PTOutput out = operator.getOutputs().get(0);
          Assert.assertEquals("unifier sinks " + operator.getName(), 1, out.sinks.size());
          PTOperator.PTInput idInput = out.sinks.get(0);
          LogicalPlan.OperatorMeta idMeta = idInput.target.getOperatorMeta();
          Operator.InputPort<?> idInputPort = null;
          if (idMeta == n2meta) {
            idInputPort = node2.inport1;
          } else if (idMeta == n3meta) {
            idInputPort = node3.inport1;
          }
          List<OperatorDeployInfo.InputDeployInfo> idis = odi.inputs;
          for (OperatorDeployInfo.InputDeployInfo idi : idis) {
            String id = operator.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(idMeta, idInputPort, idi.streamCodecs, id, plan);
          }
        }
      }
    }
  }

  @Test
  public void testInlineStreamCodec()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe);

    dag.addStream("n1n2n3", node1.outport1, node2.inport1, node3.inport1);

    // Relying on container max count for the manager to layout node1 and node3 in the
    // same container in inline fashion and node2 in a separate container
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 2);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 2, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);

    LogicalPlan.OperatorMeta nonInlineMeta = null;

    for (int i = 0; i < containers.size(); ++i) {
      PTContainer container = containers.get(i);
      List<PTOperator> operators = container.getOperators();
      if (operators.size() == 1) {
        nonInlineMeta = operators.get(0).getOperatorMeta();
        break;
      }
    }

    Assert.assertNotNull("non inline operator meta is null", nonInlineMeta);
    GenericTestOperator nonInlineOperator = null;
    Operator.InputPort<?> niInputPort = null;

    if (nonInlineMeta.getName().equals("node2")) {
      nonInlineOperator = node2;
      niInputPort = node2.inport1;
    } else if (nonInlineMeta.getName().equals("node3")) {
      nonInlineOperator = node3;
      niInputPort = node3.inport1;
    }

    Assert.assertNotNull("non inline operator is null", nonInlineOperator);

    OperatorDeployInfo n1di = getSingleOperatorDeployInfo(node1, dnm);

    OperatorDeployInfo.OutputDeployInfo n1odi = getOutputDeployInfo(n1di, n1meta.getMeta(node1.outport1));
    String id = n1meta.getName() + " " + n1odi.portName;
    Assert.assertEquals("number stream codecs " + id, n1odi.streamCodecs.size(), 1);
    checkPresentStreamCodec(nonInlineMeta, niInputPort, n1odi.streamCodecs, id, plan);

    OperatorDeployInfo odi = getSingleOperatorDeployInfo(nonInlineOperator, dnm);

    OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, nonInlineMeta.getMeta(niInputPort));
    id = nonInlineMeta.getName() + " " + idi.portName;
    Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
    checkPresentStreamCodec(nonInlineMeta, niInputPort, idi.streamCodecs, id, plan);

    /*
    OperatorDeployInfo n3di = getSingleOperatorDeployInfo(node3, node3.getName(), dnm);

    OperatorDeployInfo.InputDeployInfo n3idi = getInputDeployInfo(n3di, n3meta.getMeta(node3.inport1));
    id = n3meta.getName() + " " + n3idi.portName;
    Assert.assertEquals("number stream codecs " + id, n3idi.streamCodecs.size(), 1);
    streamIdentifier.operName = n3meta.getName();
    streamIdentifier.portName = n3meta.getMeta(node3.inport1).getPortName();
    checkStreamCodecInfo(n3idi.streamCodecs, id, streamIdentifier, serDe2);
    */
  }

  @Test
  public void testCascadingStreamCodec()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    dag.setOperatorAttribute(node1, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    dag.setOutputPortAttribute(node1.outport1, Context.PortContext.UNIFIER_LIMIT, 2);
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    TestStreamCodec2 serDe2 = new TestStreamCodec2();
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe2);

    dag.addStream("n1n2n3", node1.outport1, node2.inport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("number containers", 7, containers.size());

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (i + 1));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    for (PTContainer container : containers) {
      List<PTOperator> operators = container.getOperators();
      for (PTOperator operator : operators) {
        if (!operator.isUnifier()) {
          if (operator.getOperatorMeta() == n1meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n1meta.getName(), dnm);

            OperatorDeployInfo.OutputDeployInfo otdi = getOutputDeployInfo(odi, n1meta.getMeta(node1.outport1));
            String id = n1meta.getName() + " " + otdi.portName;
            Assert.assertEquals("number stream codecs " + id, otdi.streamCodecs.size(), 2);
            checkPresentStreamCodec(n2meta, node2.inport1, otdi.streamCodecs, id, plan);
            checkPresentStreamCodec(n3meta, node3.inport1, otdi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n2meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
            String id = n2meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);
          } else if (operator.getOperatorMeta() == n3meta) {
            OperatorDeployInfo odi = getOperatorDeployInfo(operator, n3meta.getName(), dnm);

            OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n3meta.getMeta(node3.inport1));
            String id = n3meta.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(n3meta, node3.inport1, idi.streamCodecs, id, plan);
          }
        } else {
          OperatorDeployInfo odi = getOperatorDeployInfo(operator, operator.getName(), dnm);
          Assert.assertEquals("unifier outputs " + operator.getName(), 1, operator.getOutputs().size());
          PTOperator.PTOutput out = operator.getOutputs().get(0);
          Assert.assertEquals("unifier sinks " + operator.getName(), 1, out.sinks.size());
          PTOperator.PTInput idInput = out.sinks.get(0);
          LogicalPlan.OperatorMeta idMeta = StreamingContainerAgent.getIdentifyingInputPortMeta(idInput).getOperatorMeta();
          Operator.InputPort<?> idInputPort = null;
          if (idMeta == n2meta) {
            idInputPort = node2.inport1;
          } else if (idMeta == n3meta) {
            idInputPort = node3.inport1;
          }
          List<OperatorDeployInfo.InputDeployInfo> idis = odi.inputs;
          for (OperatorDeployInfo.InputDeployInfo idi : idis) {
            String id = operator.getName() + " " + idi.portName;
            Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
            checkPresentStreamCodec(idMeta, idInputPort, idi.streamCodecs, id, plan);
          }
        }
      }
    }
  }

  @Test
  public void testDynamicPartitioningStreamCodec()
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    dag.setOperatorAttribute(node1, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    dag.setOperatorAttribute(node1, Context.OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)new PartitioningTest.PartitionLoadWatch()));
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    dag.setOperatorAttribute(node2, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    dag.setOperatorAttribute(node2, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitioningTest.PartitionLoadWatch()}));
    TestStreamCodec serDe = new TestStreamCodec();
    dag.setInputPortAttribute(node2.inport1, Context.PortContext.STREAM_CODEC, serDe);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    dag.setOperatorAttribute(node3, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));
    dag.setOperatorAttribute(node3, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitioningTest.PartitionLoadWatch()}));
    TestStreamCodec serDe2 = new TestStreamCodec();
    dag.setInputPortAttribute(node3.inport1, Context.PortContext.STREAM_CODEC, serDe2);

    dag.addStream("n1n2n3", node1.outport1, node2.inport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    StramTestSupport.MemoryStorageAgent msa = new StramTestSupport.MemoryStorageAgent();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    int lastId = 0;

    for (int i = 0; i < containers.size(); ++i) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (++lastId));
    }

    LogicalPlan.OperatorMeta n1meta = dag.getMeta(node1);
    LogicalPlan.OperatorMeta n2meta = dag.getMeta(node2);
    LogicalPlan.OperatorMeta n3meta = dag.getMeta(node3);

    // Sanity check that physical operators have been allocated for n1meta and n2meta
    Assert.assertEquals("number operators " + n1meta.getName(), 2, plan.getOperators(n1meta).size());
    Assert.assertEquals("number operators " + n2meta.getName(), 3, plan.getOperators(n2meta).size());
    Assert.assertEquals("number operators " + n3meta.getName(), 3, plan.getOperators(n3meta).size());

    // Test Dynamic change
    // for M x N partition
    // scale down N (node2) from 3 to 2 and then from 2 to 1
    for (int i = 0; i < 2; i++) {
      markAllOperatorsActive(plan);
      List<PTOperator> ptos = plan.getOperators(n2meta);
      for (PTOperator ptOperator : ptos) {
        PartitioningTest.PartitionLoadWatch.put(ptOperator, -1);
        plan.onStatusUpdate(ptOperator);
      }

      dnm.processEvents();
      lastId = assignNewContainers(dnm, lastId);

      List<PTOperator> operators = plan.getOperators(n2meta);
      List<PTOperator> upstreamOperators = new ArrayList<>();
      for (PTOperator operator : operators) {
        upstreamOperators.addAll(operator.upstreamMerge.values());
        /*
        OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

        OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
        String id = n2meta.getName() + " " + idi.portName;
        Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
        checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);
        */
      }

      Assert.assertEquals("Number of unifiers ", 2 - i, upstreamOperators.size());
    }

    // scale down N (node3) from 3 to 2 and then from 2 to 1
    for (int i = 0; i < 2; i++) {
      markAllOperatorsActive(plan);
      List<PTOperator> ptos = plan.getOperators(n3meta);
      for (PTOperator ptOperator : ptos) {
        PartitioningTest.PartitionLoadWatch.put(ptOperator, -1);
        plan.onStatusUpdate(ptOperator);
      }

      dnm.processEvents();
      lastId = assignNewContainers(dnm, lastId);

      List<PTOperator> operators = plan.getOperators(n3meta);
      List<PTOperator> upstreamOperators = new ArrayList<>();
      for (PTOperator operator : operators) {
        upstreamOperators.addAll(operator.upstreamMerge.values());
      }

      Assert.assertEquals("Number of unifiers ", 2 - i, upstreamOperators.size());
    }

    // Check that different unifiers were created for the two output operators with different codecs
    // even though there are only one partition of each one
    Set<PTOperator> unifiers = getUnifiers(plan);
    Assert.assertEquals("Number of unifiers ", 2, unifiers.size());

    // scale up N (node2) from 1 to 2 and then from 2 to 3
    for (int i = 0; i < 2; i++) {
      markAllOperatorsActive(plan);
      PTOperator o2p1 = plan.getOperators(n2meta).get(0);

      PartitioningTest.PartitionLoadWatch.put(o2p1, 1);

      plan.onStatusUpdate(o2p1);

      dnm.processEvents();

      lastId = assignNewContainers(dnm, lastId);

      List<PTOperator> operators = plan.getOperators(n2meta);
      List<PTOperator> upstreamOperators = new ArrayList<>();
      for (PTOperator operator : operators) {
        upstreamOperators.addAll(operator.upstreamMerge.values());
        /*
        if (operator.getState() != PTOperator.State.ACTIVE) {
          OperatorDeployInfo odi = getOperatorDeployInfo(operator, n2meta.getName(), dnm);

          OperatorDeployInfo.InputDeployInfo idi = getInputDeployInfo(odi, n2meta.getMeta(node2.inport1));
          String id = n2meta.getName() + " " + idi.portName;
          Assert.assertEquals("number stream codecs " + id, idi.streamCodecs.size(), 1);
          checkPresentStreamCodec(n2meta, node2.inport1, idi.streamCodecs, id, plan);
        }
        */
      }

      Assert.assertEquals("Number of unifiers ", 2 + i, upstreamOperators.size());
    }

    // scale down M to 1
    {
      markAllOperatorsActive(plan);
      for (PTOperator o1p : plan.getOperators(n1meta)) {
        PartitioningTest.PartitionLoadWatch.put(o1p, -1);
        plan.onStatusUpdate(o1p);
      }

      dnm.processEvents();

      lastId = assignNewContainers(dnm, lastId);

      unifiers = getUnifiers(plan);
      Assert.assertEquals("Number of unifiers", 0, unifiers.size());
    }

    // scale up M to 2
    {
      markAllOperatorsActive(plan);
      for (PTOperator o1p : plan.getOperators(n1meta)) {
        PartitioningTest.PartitionLoadWatch.put(o1p, 1);
        plan.onStatusUpdate(o1p);
      }

      dnm.processEvents();

      lastId = assignNewContainers(dnm, lastId);

      unifiers = getUnifiers(plan);
      Assert.assertEquals("Number of unifiers", 4, unifiers.size());
    }
  }

  private int assignNewContainers(StreamingContainerManager dnm, int lastId)
  {
    PhysicalPlan plan = dnm.getPhysicalPlan();
    List<PTContainer> containers = plan.getContainers();

    int numPending = 0;

    for (PTContainer container : containers) {
      if (container.getState() == PTContainer.State.NEW) {
        numPending++;
      }
    }

    for (int j = 0; j < numPending; ++j) {
      StreamingContainerManagerTest.assignContainer(dnm, "container" + (++lastId));
    }
    return lastId;
  }

  private void markAllOperatorsActive(PhysicalPlan plan)
  {
    for (PTContainer container : plan.getContainers()) {
      for (PTOperator operator : container.getOperators()) {
        operator.setState(PTOperator.State.ACTIVE);
      }
    }
  }

  private Set<PTOperator> getUnifiers(PhysicalPlan plan)
  {
    Set<PTOperator> unifiers = new HashSet<>();
    for (PTContainer container : plan.getContainers()) {
      for (PTOperator operator : container.getOperators()) {
        if (operator.isUnifier()) {
          unifiers.add(operator);
        }
      }
    }
    return unifiers;
  }

  private void checkPresentStreamCodec(LogicalPlan.OperatorMeta operatorMeta, Operator.InputPort<?> inputPort,
      Map<Integer, StreamCodec<?>> streamCodecs,
      String id, PhysicalPlan plan)
  {
    StreamCodec<?> streamCodec = operatorMeta.getMeta(inputPort).getStreamCodec();
    Assert.assertTrue("stream codec identifier not present" + id, isStrCodecPresent(streamCodec, plan));
    Integer streamCodecIdentifier = plan.getStreamCodecIdentifier(streamCodec);
    checkPresentStreamCodecInfo(streamCodecs, id, streamCodecIdentifier, streamCodec);
  }

  private void checkPresentStreamCodecInfo(Map<Integer, StreamCodec<?>> streamCodecs, String id,
      Integer streamCodecIdentifier, StreamCodec<?> streamCodecInfo)
  {
    StreamCodec<?> opStreamCodecInfo = streamCodecs.get(streamCodecIdentifier);
    Assert.assertNotNull("stream codec info null " + id, opStreamCodecInfo);
    Assert.assertEquals("stream codec not same " + id, opStreamCodecInfo, streamCodecInfo);
  }

  private OperatorDeployInfo getSingleOperatorDeployInfo(Operator oper, StreamingContainerManager scm)
  {
    LogicalPlan dag = scm.getLogicalPlan();
    String id = dag.getMeta(oper).toString();
    PhysicalPlan plan = scm.getPhysicalPlan();
    List<PTOperator> operators = plan.getOperators(dag.getMeta(oper));
    Assert.assertEquals("number of operators " + id, 1, operators.size());

    PTOperator operator = operators.get(0);
    return getOperatorDeployInfo(operator, id, scm);
  }

  private OperatorDeployInfo getOperatorDeployInfo(PTOperator operator, String id, StreamingContainerManager scm)
  {
    String containerId = operator.getContainer().getExternalId();

    List<OperatorDeployInfo> cdi = StreamingContainerManagerTest.getDeployInfo(scm.getContainerAgent(containerId));

    OperatorDeployInfo odi = null;
    for (OperatorDeployInfo iodi : cdi) {
      if (iodi.id == operator.getId()) {
        odi = iodi;
        break;
      }
    }

    Assert.assertNotNull(id + " assigned to " + containerId + " deploy info is null", odi);
    return odi;
  }

  private OperatorDeployInfo.InputDeployInfo getInputDeployInfo(OperatorDeployInfo odi, LogicalPlan.InputPortMeta
      portMeta)
  {
    OperatorDeployInfo.InputDeployInfo idi = null;
    List<OperatorDeployInfo.InputDeployInfo> inputs = odi.inputs;
    for (OperatorDeployInfo.InputDeployInfo input : inputs) {
      if (input.portName.equals(portMeta.getPortName())) {
        idi = input;
        break;
      }
    }
    Assert.assertNotNull("input deploy info " + portMeta.getPortName(), idi);
    return idi;
  }

  private OperatorDeployInfo.OutputDeployInfo getOutputDeployInfo(OperatorDeployInfo odi, LogicalPlan.OutputPortMeta portMeta)
  {
    OperatorDeployInfo.OutputDeployInfo otdi = null;
    List<OperatorDeployInfo.OutputDeployInfo> outputs = odi.outputs;
    for (OperatorDeployInfo.OutputDeployInfo output : outputs) {
      if (output.portName.equals(portMeta.getPortName())) {
        otdi = output;
        break;
      }
    }
    Assert.assertNotNull("output deploy info " + portMeta.getPortName(), otdi);
    return otdi;
  }

  // For tests so that it doesn't trigger assignment of a new id
  public boolean isStrCodecPresent(StreamCodec<?> streamCodecInfo, PhysicalPlan plan)
  {
    return plan.getStreamCodecIdentifiers().containsKey(streamCodecInfo);
  }

  public static class TestStreamCodec extends DefaultStatefulStreamCodec<Object>
  {

    @Override
    public int getPartition(Object o)
    {
      return o.hashCode() / 2;
    }
  }

  public static class TestStreamCodec2 extends DefaultStatefulStreamCodec<Object>
  {

    @Override
    public int getPartition(Object o)
    {
      return o.hashCode() / 3;
    }
  }

  public static class DefaultTestStreamCodec extends DefaultStatefulStreamCodec<Object> implements Serializable
  {
    private static final long serialVersionUID = 1L;
  }

  public static class DefaultCodecOperator extends GenericTestOperator
  {
    private static final DefaultTestStreamCodec codec = new DefaultTestStreamCodec();

    @InputPortFieldAnnotation(optional = true)
    public final transient InputPort<Object> inportWithCodec = new DefaultInputPort<Object>()
    {
      @Override
      public StreamCodec<Object> getStreamCodec()
      {
        return codec;
      }

      @Override
      public final void process(Object payload)
      {
      }

    };
  }
}

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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.codehaus.jettison.json.JSONObject;
import org.eclipse.jetty.websocket.WebSocket;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.Stats.OperatorStats.PortStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.metric.AutoMetricBuiltInTransport;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.FSStorageAgent;
import com.datatorrent.stram.MockContainer.MockOperatorStats;
import com.datatorrent.stram.StreamingContainerAgent.ContainerStartRequest;
import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.api.AppDataSource;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.ContainerContext;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.api.OperatorDeployInfo.InputDeployInfo;
import com.datatorrent.stram.api.OperatorDeployInfo.OutputDeployInfo;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat.DeployState;
import com.datatorrent.stram.appdata.AppDataPushAgent;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;
import com.datatorrent.stram.engine.DefaultUnifier;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestAppDataQueryOperator;
import com.datatorrent.stram.engine.TestAppDataResultOperator;
import com.datatorrent.stram.engine.TestAppDataSourceOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.TestPlanContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.physical.OperatorStatus.PortStatus;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.plan.physical.PhysicalPlanTest;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.EmbeddedWebSocketServer;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;
import com.datatorrent.stram.tuple.Tuple;
import com.datatorrent.stram.webapp.LogicalOperatorInfo;

public class StreamingContainerManagerTest
{
  @Rule
  public TestMeta testMeta = new TestMeta();

  private LogicalPlan dag;

  @Before
  public void setup()
  {
    dag = StramTestSupport.createDAG(testMeta);
  }

  @Test
  public void testDeployInfoSerialization() throws Exception
  {
    OperatorDeployInfo ndi = new OperatorDeployInfo();
    ndi.name = "node1";
    ndi.type = OperatorDeployInfo.OperatorType.GENERIC;
    ndi.id = 1;
    ndi.contextAttributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    ndi.contextAttributes.put(OperatorContext.SPIN_MILLIS, 100);

    OperatorDeployInfo.InputDeployInfo input = new OperatorDeployInfo.InputDeployInfo();
    input.declaredStreamId = "streamToNode";
    input.portName = "inputPortNameOnNode";
    input.sourceNodeId = 99;

    ndi.inputs = new ArrayList<>();
    ndi.inputs.add(input);

    OperatorDeployInfo.OutputDeployInfo output = new OperatorDeployInfo.OutputDeployInfo();
    output.declaredStreamId = "streamFromNode";
    output.portName = "outputPortNameOnNode";

    ndi.outputs = new ArrayList<>();
    ndi.outputs.add(output);

    ContainerHeartbeatResponse scc = new ContainerHeartbeatResponse();
    scc.deployRequest = Collections.singletonList(ndi);

    DataOutputByteBuffer out = new DataOutputByteBuffer();
    scc.write(out);

    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(out.getData());

    ContainerHeartbeatResponse clone = new ContainerHeartbeatResponse();
    clone.readFields(in);

    Assert.assertNotNull(clone.deployRequest);
    Assert.assertEquals(1, clone.deployRequest.size());
    OperatorDeployInfo ndiClone = clone.deployRequest.get(0);
    Assert.assertEquals("name", ndi.name, ndiClone.name);
    Assert.assertEquals("type", ndi.type, ndiClone.type);

    String nodeToString = ndi.toString();
    Assert.assertTrue(nodeToString.contains(input.portName));
    Assert.assertTrue(nodeToString.contains(output.portName));

    Assert.assertEquals("contextAttributes " + ndiClone.contextAttributes, Integer.valueOf(100), ndiClone.contextAttributes.get(OperatorContext.SPIN_MILLIS));

  }

  @Test
  public void testGenerateDeployInfo()
  {
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    GenericTestOperator o4 = dag.addOperator("o4", GenericTestOperator.class);

    dag.setOutputPortAttribute(o1.outport,PortContext.BUFFER_MEMORY_MB,256);
    dag.addStream("o1.outport", o1.outport, o2.inport1);
    dag.setOutputPortAttribute(o1.outport, PortContext.SPIN_MILLIS, 99);

    dag.addStream("o2.outport1", o2.outport1, o3.inport1)
        .setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("o3.outport1", o3.outport1, o4.inport1)
        .setLocality(Locality.THREAD_LOCAL);

    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());
    Assert.assertEquals("number operators", 4, dag.getAllOperators().size());
    Assert.assertEquals("number root operators", 1, dag.getRootOperators().size());

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    Assert.assertEquals("number containers", 2, dnm.getPhysicalPlan().getContainers().size());

    dnm.assignContainer(new ContainerResource(0, "container1Id", "host1", 1024, 0, null), InetSocketAddress.createUnresolved("host1", 9001));
    dnm.assignContainer(new ContainerResource(0, "container2Id", "host2", 1024, 0, null), InetSocketAddress.createUnresolved("host2", 9002));

    StreamingContainerAgent sca1 = dnm.getContainerAgent(dnm.getPhysicalPlan().getContainers().get(0).getExternalId());
    StreamingContainerAgent sca2 = dnm.getContainerAgent(dnm.getPhysicalPlan().getContainers().get(1).getExternalId());

    Assert.assertEquals("", dnm.getPhysicalPlan().getContainers().get(0), sca1.container);
    Assert.assertEquals("", PTContainer.State.ALLOCATED, sca1.container.getState());
    List<OperatorDeployInfo> c1 = sca1.getDeployInfoList(sca1.container.getOperators());

    Assert.assertEquals("number operators assigned to c1", 1, c1.size());
    OperatorDeployInfo o1DI = getNodeDeployInfo(c1, dag.getMeta(o1));
    Assert.assertNotNull(o1 + " assigned to " + sca1.container.getExternalId(), o1DI);
    Assert.assertEquals("type " + o1DI, OperatorDeployInfo.OperatorType.INPUT, o1DI.type);
    Assert.assertEquals("inputs " + o1DI.name, 0, o1DI.inputs.size());
    Assert.assertEquals("outputs " + o1DI.name, 1, o1DI.outputs.size());
    Assert.assertNotNull("contextAttributes " + o1DI.name, o1DI.contextAttributes);

    OutputDeployInfo c1o1outport = o1DI.outputs.get(0);
    Assert.assertNotNull("stream connection for container1", c1o1outport);
    Assert.assertEquals("stream connection for container1", "o1.outport", c1o1outport.declaredStreamId);
    Assert.assertEquals("stream connects to upstream host", sca1.container.host, c1o1outport.bufferServerHost);
    Assert.assertEquals("stream connects to upstream port", sca1.container.bufferServerAddress.getPort(), c1o1outport.bufferServerPort);
    Assert.assertNotNull("contextAttributes " + c1o1outport, c1o1outport.contextAttributes);
    Assert.assertEquals("contextAttributes " + c1o1outport,  Integer.valueOf(99), c1o1outport.contextAttributes.get(PortContext.SPIN_MILLIS));

    List<OperatorDeployInfo> c2 = sca2.getDeployInfoList(sca2.container.getOperators());
    Assert.assertEquals("number operators assigned to container", 3, c2.size());
    OperatorDeployInfo o2DI = getNodeDeployInfo(c2, dag.getMeta(o2));
    OperatorDeployInfo o3DI = getNodeDeployInfo(c2, dag.getMeta(o3));
    Assert.assertNotNull(dag.getMeta(o2) + " assigned to " + sca2.container.getExternalId(), o2DI);
    Assert.assertNotNull(dag.getMeta(o3) + " assigned to " + sca2.container.getExternalId(), o3DI);

    Assert.assertTrue("The buffer server memory for container 1", 256 == sca1.getInitContext().getValue(ContainerContext.BUFFER_SERVER_MB));
    Assert.assertTrue("The buffer server memory for container 2", 0 == sca2.getInitContext().getValue(ContainerContext.BUFFER_SERVER_MB));
    // buffer server input o2 from o1
    InputDeployInfo c2o2i1 = getInputDeployInfo(o2DI, "o1.outport");
    Assert.assertNotNull("stream connection for container2", c2o2i1);
    Assert.assertEquals("stream connects to upstream host", sca1.container.host, c2o2i1.bufferServerHost);
    Assert.assertEquals("stream connects to upstream port", sca1.container.bufferServerAddress.getPort(), c2o2i1.bufferServerPort);
    Assert.assertEquals("portName " + c2o2i1, dag.getMeta(o2).getMeta(o2.inport1).getPortName(), c2o2i1.portName);
    Assert.assertNull("partitionKeys " + c2o2i1, c2o2i1.partitionKeys);
    Assert.assertEquals("sourceNodeId " + c2o2i1, o1DI.id, c2o2i1.sourceNodeId);
    Assert.assertEquals("sourcePortName " + c2o2i1, TestGeneratorInputOperator.OUTPUT_PORT, c2o2i1.sourcePortName);
    Assert.assertNotNull("contextAttributes " + c2o2i1, c2o2i1.contextAttributes);

    // inline input o3 from o2
    InputDeployInfo c2o3i1 = getInputDeployInfo(o3DI, "o2.outport1");
    Assert.assertNotNull("input from o2.outport1", c2o3i1);
    Assert.assertEquals("portName " + c2o3i1, GenericTestOperator.IPORT1, c2o3i1.portName);
    Assert.assertNotNull("stream connection for container2", c2o3i1);
    Assert.assertNull("bufferServerHost " + c2o3i1, c2o3i1.bufferServerHost);
    Assert.assertEquals("bufferServerPort " + c2o3i1, 0, c2o3i1.bufferServerPort);
    Assert.assertNull("partitionKeys " + c2o3i1, c2o3i1.partitionKeys);
    Assert.assertEquals("sourceNodeId " + c2o3i1, o2DI.id, c2o3i1.sourceNodeId);
    Assert.assertEquals("sourcePortName " + c2o3i1, GenericTestOperator.OPORT1, c2o3i1.sourcePortName);
    Assert.assertEquals("locality " + c2o3i1, Locality.CONTAINER_LOCAL, c2o3i1.locality);

    // THREAD_LOCAL o4.inport1
    OperatorDeployInfo o4DI = getNodeDeployInfo(c2, dag.getMeta(o4));
    Assert.assertNotNull(dag.getMeta(o4) + " assigned to " + sca2.container.getExternalId(), o4DI);
    InputDeployInfo c2o4i1 = getInputDeployInfo(o4DI, "o3.outport1");
    Assert.assertNotNull("input from o3.outport1", c2o4i1);
    Assert.assertEquals("portName " + c2o4i1, GenericTestOperator.IPORT1, c2o4i1.portName);
    Assert.assertNotNull("stream connection for container2", c2o4i1);
    Assert.assertNull("bufferServerHost " + c2o4i1, c2o4i1.bufferServerHost);
    Assert.assertEquals("bufferServerPort " + c2o4i1, 0, c2o4i1.bufferServerPort);
    Assert.assertNull("partitionKeys " + c2o4i1, c2o4i1.partitionKeys);
    Assert.assertEquals("sourceNodeId " + c2o4i1, o3DI.id, c2o4i1.sourceNodeId);
    Assert.assertEquals("sourcePortName " + c2o4i1, GenericTestOperator.OPORT1, c2o4i1.sourcePortName);
    Assert.assertEquals("locality " + c2o4i1, Locality.THREAD_LOCAL, c2o4i1.locality);

  }

  @Test
  public void testStaticPartitioning()
  {
    //
    //            ,---> node2----,
    //            |              |
    //    node1---+---> node2----+---> unifier | node3
    //            |              |
    //            '---> node2----'
    //
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    PhysicalPlanTest.PartitioningTestOperator node2 = dag.addOperator("node2", PhysicalPlanTest.PartitioningTestOperator.class);
    node2.setPartitionCount(3);
    dag.setOperatorAttribute(node2, OperatorContext.SPIN_MILLIS, 10); /* this should not affect anything materially */
    dag.setOutputPortAttribute(node2.outport1, PortContext.QUEUE_CAPACITY, 1111);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);
    dag.setInputPortAttribute(node3.inport1, PortContext.QUEUE_CAPACITY, 2222);

    LogicalPlan.StreamMeta n1n2 = dag.addStream("n1n2", node1.outport1, node2.inport1);
    LogicalPlan.StreamMeta n2n3 = dag.addStream("n2n3", node2.outport1, node3.inport1);

    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, Integer.MAX_VALUE);
    MemoryStorageAgent msa = new MemoryStorageAgent();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, msa);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    Assert.assertEquals("number containers", 5, plan.getContainers().size());
    List<StreamingContainerAgent> containerAgents = Lists.newArrayList();
    for (int i = 0; i < plan.getContainers().size(); i++) {
      containerAgents.add(assignContainer(dnm, "container" + (i + 1)));
    }

    PTContainer c = plan.getOperators(dag.getMeta(node1)).get(0).getContainer();
    StreamingContainerAgent sca1 = dnm.getContainerAgent(c.getExternalId());
    List<OperatorDeployInfo> c1 = getDeployInfo(sca1);
    Assert.assertEquals("number operators assigned to container", 1, c1.size());
    Assert.assertTrue(dag.getMeta(node2) + " assigned to " + sca1.container.getExternalId(), containsNodeContext(c1, dag.getMeta(node1)));

    List<PTOperator> o2Partitions = plan.getOperators(dag.getMeta(node2));
    Assert.assertEquals("number partitions", TestStaticPartitioningSerDe.partitions.length, o2Partitions.size());

    for (int i = 0; i < o2Partitions.size(); i++) {
      String containerId = o2Partitions.get(i).getContainer().getExternalId();
      List<OperatorDeployInfo> cc = getDeployInfo(dnm.getContainerAgent(containerId));
      Assert.assertEquals("number operators assigned to container", 1, cc.size());
      Assert.assertTrue(dag.getMeta(node2) + " assigned to " + containerId, containsNodeContext(cc, dag.getMeta(node2)));

      // n1n2 in, mergeStream out
      OperatorDeployInfo ndi = cc.get(0);
      Assert.assertEquals("type " + ndi, OperatorDeployInfo.OperatorType.GENERIC, ndi.type);
      Assert.assertEquals("inputs " + ndi, 1, ndi.inputs.size());
      Assert.assertEquals("outputs " + ndi, 1, ndi.outputs.size());

      InputDeployInfo nidi = ndi.inputs.get(0);
      Assert.assertEquals("stream " + nidi, n1n2.getName(), nidi.declaredStreamId);
      Assert.assertEquals("partition for " + containerId, Sets.newHashSet(node2.partitionKeys[i]), nidi.partitionKeys);
      Assert.assertEquals("number stream codecs for " + nidi, 1, nidi.streamCodecs.size());
    }

    List<OperatorDeployInfo> cUnifier = getDeployInfo(dnm.getContainerAgent(plan.getOperators(dag.getMeta(node3)).get(0).getContainer().getExternalId()));
    Assert.assertEquals("number operators " + cUnifier, 2, cUnifier.size());

    OperatorDeployInfo mergeNodeDI = getNodeDeployInfo(cUnifier, dag.getMeta(node2).getMeta(node2.outport1).getUnifierMeta());
    Assert.assertNotNull("unifier for " + node2, mergeNodeDI);
    Assert.assertEquals("type " + mergeNodeDI, OperatorDeployInfo.OperatorType.UNIFIER, mergeNodeDI.type);
    Assert.assertEquals("inputs " + mergeNodeDI, 3, mergeNodeDI.inputs.size());
    List<Integer> sourceNodeIds = Lists.newArrayList();
    for (InputDeployInfo nidi : mergeNodeDI.inputs) {
      Assert.assertEquals("streamName " + nidi, n2n3.getName(), nidi.declaredStreamId);
      String mergePortName = "<merge#" +  dag.getMeta(node2).getMeta(node2.outport1).getPortName() + ">";
      Assert.assertEquals("portName " + nidi, mergePortName, nidi.portName);
      Assert.assertNotNull("sourceNodeId " + nidi, nidi.sourceNodeId);
      Assert.assertNotNull("contextAttributes " + nidi, nidi.contextAttributes);
      Assert.assertEquals("contextAttributes ", new Integer(1111), nidi.getValue(PortContext.QUEUE_CAPACITY));
      sourceNodeIds.add(nidi.sourceNodeId);
    }
    for (PTOperator node : dnm.getPhysicalPlan().getOperators(dag.getMeta(node2))) {
      Assert.assertTrue(sourceNodeIds + " contains " + node.getId(), sourceNodeIds.contains(node.getId()));
    }

    Assert.assertEquals("outputs " + mergeNodeDI, 1, mergeNodeDI.outputs.size());
    for (OutputDeployInfo odi : mergeNodeDI.outputs) {
      Assert.assertNotNull("contextAttributes " + odi, odi.contextAttributes);
      Assert.assertEquals("contextAttributes ", new Integer(2222), odi.getValue(PortContext.QUEUE_CAPACITY));
    }

    try {
      Object operator = msa.load(mergeNodeDI.id, Stateless.WINDOW_ID);
      Assert.assertTrue("" + operator, operator instanceof DefaultUnifier);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    // node3 container
    c = plan.getOperators(dag.getMeta(node3)).get(0).getContainer();
    List<OperatorDeployInfo> cmerge = getDeployInfo(dnm.getContainerAgent(c.getExternalId()));
    Assert.assertEquals("number operators " + cmerge, 2, cmerge.size());

    OperatorDeployInfo node3DI = getNodeDeployInfo(cmerge,  dag.getMeta(node3));
    Assert.assertNotNull(dag.getMeta(node3) + " assigned", node3DI);
    Assert.assertEquals("inputs " + node3DI, 1, node3DI.inputs.size());
    InputDeployInfo node3In = node3DI.inputs.get(0);
    Assert.assertEquals("streamName " + node3In, n2n3.getName(), node3In.declaredStreamId);
    Assert.assertEquals("portName " + node3In, dag.getMeta(node3).getMeta(node3.inport1).getPortName(), node3In.portName);
    Assert.assertNotNull("sourceNodeId " + node3DI, node3In.sourceNodeId);
    Assert.assertEquals("sourcePortName " + node3DI, mergeNodeDI.outputs.get(0).portName, node3In.sourcePortName);
  }

  private static void shutdownOperator(StreamingContainerManager scm, PTOperator p1, PTOperator p2)
  {
    assignContainer(scm, "c1");
    assignContainer(scm, "c2");

    ContainerHeartbeat c1hb = new ContainerHeartbeat();
    c1hb.setContainerStats(new ContainerStats(p1.getContainer().getExternalId()));
    scm.processHeartbeat(c1hb);

    ContainerHeartbeat c2hb = new ContainerHeartbeat();
    c2hb.setContainerStats(new ContainerStats(p2.getContainer().getExternalId()));
    scm.processHeartbeat(c2hb);

    OperatorHeartbeat o1hb = new OperatorHeartbeat();
    c1hb.getContainerStats().addNodeStats(o1hb);
    o1hb.setNodeId(p1.getId());
    o1hb.setState(DeployState.ACTIVE);
    OperatorStats o1stats = new OperatorStats();
    o1hb.getOperatorStatsContainer().add(o1stats);
    o1stats.checkpoint = new Checkpoint(2, 0, 0);
    o1stats.windowId = 3;
    scm.processHeartbeat(c1hb);
    Assert.assertEquals(PTOperator.State.ACTIVE, p1.getState());

    OperatorHeartbeat o2hb = new OperatorHeartbeat();
    c2hb.getContainerStats().addNodeStats(o2hb);
    o2hb.setNodeId(p2.getId());
    o2hb.setState(DeployState.ACTIVE);
    OperatorStats o2stats = new OperatorStats();
    o2stats.checkpoint = new Checkpoint(2, 0, 0);
    o2stats.windowId = 3;
    scm.processHeartbeat(c2hb);
    Assert.assertEquals(PTOperator.State.ACTIVE, p1.getState());
    Assert.assertEquals(PTOperator.State.ACTIVE, p2.getState());

    o1hb.setState(DeployState.SHUTDOWN);
    o1stats.checkpoint = new Checkpoint(4, 0,0);
    o1stats.windowId = 5;
    scm.processHeartbeat(c1hb);
    Assert.assertEquals(PTOperator.State.INACTIVE, p1.getState());
  }

  @Test
  public void testShutdownOperatorTimeout() throws Exception
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.addStream("s1", o1.outport1, o2.inport1);

    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());
    dag.setAttribute(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 50);
    dag.setAttribute(OperatorContext.TIMEOUT_WINDOW_COUNT, 1);

    StreamingContainerManager scm = new StreamingContainerManager(dag);

    PhysicalPlan plan = scm.getPhysicalPlan();

    PTOperator p1 = plan.getOperators(dag.getMeta(o1)).get(0);
    PTOperator p2 = plan.getOperators(dag.getMeta(o2)).get(0);

    shutdownOperator(scm, p1, p2);

    scm.monitorHeartbeat(false);
    Assert.assertTrue(scm.containerStopRequests.isEmpty());
    Thread.sleep(100);
    scm.monitorHeartbeat(false);
    Assert.assertFalse(scm.containerStopRequests.containsKey(p1.getContainer().getExternalId()));
    Assert.assertTrue(scm.containerStopRequests.containsKey(p2.getContainer().getExternalId()));
  }

  @Test
  public void testShutdownOperatorRecovery() throws Exception
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.addStream("s1", o1.outport1, o2.inport1);

    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    scm.containerStartRequests.poll();
    scm.containerStartRequests.poll();

    PhysicalPlan plan = scm.getPhysicalPlan();

    PTOperator p1 = plan.getOperators(dag.getMeta(o1)).get(0);
    PTOperator p2 = plan.getOperators(dag.getMeta(o2)).get(0);

    shutdownOperator(scm, p1, p2);

    scm.scheduleContainerRestart(p1.getContainer().getExternalId());
    ContainerStartRequest dr = scm.containerStartRequests.poll();
    Assert.assertTrue(dr.container.getOperators().contains(p1));
  }

  @Test
  public void testRecoveryOrder() throws Exception
  {
    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);

    dag.addStream("n1n2", node1.outport1, node2.inport1);
    dag.addStream("n2n3", node2.outport1, node3.inport1);

    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    Assert.assertEquals("" + scm.containerStartRequests, 2, scm.containerStartRequests.size());
    scm.containerStartRequests.clear();

    PhysicalPlan plan = scm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("" + containers, 2, plan.getContainers().size());

    PTContainer c1 = containers.get(0);
    Assert.assertEquals("c1.operators " + c1.getOperators(), 2, c1.getOperators().size());

    PTContainer c2 = containers.get(1);
    Assert.assertEquals("c2.operators " + c2.getOperators(), 1, c2.getOperators().size());

    assignContainer(scm, "container1");
    assignContainer(scm, "container2");

    StreamingContainerAgent sca1 = scm.getContainerAgent(c1.getExternalId());
    StreamingContainerAgent sca2 = scm.getContainerAgent(c2.getExternalId());
    Assert.assertEquals("", 0, countState(sca1.container, PTOperator.State.PENDING_UNDEPLOY));
    Assert.assertEquals("", 2, countState(sca1.container, PTOperator.State.PENDING_DEPLOY));

    scm.scheduleContainerRestart(c1.getExternalId());
    Assert.assertEquals("", 0, countState(sca1.container, PTOperator.State.PENDING_UNDEPLOY));
    Assert.assertEquals("", 2, countState(sca1.container, PTOperator.State.PENDING_DEPLOY));
    Assert.assertEquals("" + scm.containerStartRequests, 1, scm.containerStartRequests.size());
    ContainerStartRequest dr = scm.containerStartRequests.peek();
    Assert.assertNotNull(dr);

    Assert.assertEquals("" + sca2.container, 1, countState(sca2.container, PTOperator.State.PENDING_UNDEPLOY));
    Assert.assertEquals("" + sca2.container, 0, countState(sca2.container, PTOperator.State.PENDING_DEPLOY));

  }

  @Test
  public void testRecoveryUpstreamInline() throws Exception
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("o1o3", o1.outport1, o3.inport1);
    dag.addStream("o2o3", o2.outport1, o3.inport2);

    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan plan = scm.getPhysicalPlan();
    Assert.assertEquals(2, plan.getContainers().size());

    plan.getOperators(dag.getMeta(o1)).get(0);

    Assert.assertEquals(2, plan.getContainers().size());
    PTContainer c1 = plan.getContainers().get(0);
    Assert.assertEquals(Sets.newHashSet(plan.getOperators(dag.getMeta(o1)).get(0), plan.getOperators(dag.getMeta(o3)).get(0)), Sets.newHashSet(c1.getOperators()));
    PTContainer c2 = plan.getContainers().get(1);

    assignContainer(scm, "c1");
    assignContainer(scm, "c2");

    for (PTOperator oper : c1.getOperators()) {
      Assert.assertEquals("state " + oper, PTOperator.State.PENDING_DEPLOY, oper.getState());
    }
    scm.scheduleContainerRestart(c2.getExternalId());
    for (PTOperator oper : c1.getOperators()) {
      Assert.assertEquals("state " + oper, PTOperator.State.PENDING_UNDEPLOY, oper.getState());
    }

  }

  @Test
  public void testCheckpointWindowIds() throws Exception
  {
    FSStorageAgent sa = new FSStorageAgent(testMeta.getPath(), null);

    long[] windowIds = new long[]{123L, 345L, 234L};
    for (long windowId : windowIds) {
      sa.save(windowId, 1, windowId);
    }

    Arrays.sort(windowIds);
    long[] windowsIds = sa.getWindowIds(1);
    Arrays.sort(windowsIds);
    Assert.assertArrayEquals("Saved windowIds", windowIds, windowsIds);
  }

  @Test
  public void testAsyncCheckpointWindowIds() throws Exception
  {
    AsyncFSStorageAgent sa = new AsyncFSStorageAgent(testMeta.getPath(), null);

    long[] windowIds = new long[]{123L, 345L, 234L};
    for (long windowId : windowIds) {
      sa.save(windowId, 1, windowId);
      sa.copyToHDFS(1, windowId);
    }

    Arrays.sort(windowIds);
    long[] windowsIds = sa.getWindowIds(1);
    Arrays.sort(windowsIds);
    Assert.assertArrayEquals("Saved windowIds", windowIds, windowsIds);
  }

  @Test
  public void testProcessHeartbeat() throws Exception
  {
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitioningTest.PartitionLoadWatch()}));
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan plan = scm.getPhysicalPlan();
    Assert.assertEquals("number required containers", 1, plan.getContainers().size());

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);

    // assign container
    String containerId = "container1";
    StreamingContainerAgent sca = scm.assignContainer(new ContainerResource(0, containerId, "localhost", 512, 0, null), InetSocketAddress.createUnresolved("localhost", 0));
    Assert.assertNotNull(sca);

    Assert.assertEquals(PTContainer.State.ALLOCATED, o1p1.getContainer().getState());
    Assert.assertEquals(PTOperator.State.PENDING_DEPLOY, o1p1.getState());

    ContainerStats cstats = new ContainerStats(containerId);
    ContainerHeartbeat hb = new ContainerHeartbeat();
    hb.setContainerStats(cstats);

    ContainerHeartbeatResponse chr = scm.processHeartbeat(hb); // get deploy request
    Assert.assertNotNull(chr.deployRequest);
    Assert.assertEquals("" + chr.deployRequest, 1, chr.deployRequest.size());
    Assert.assertEquals(PTContainer.State.ACTIVE, o1p1.getContainer().getState());
    Assert.assertEquals("state " + o1p1, PTOperator.State.PENDING_DEPLOY, o1p1.getState());

    // first operator heartbeat
    OperatorHeartbeat ohb = new OperatorHeartbeat();
    ohb.setNodeId(o1p1.getId());
    ohb.setState(OperatorHeartbeat.DeployState.ACTIVE);
    OperatorStats stats = new OperatorStats();
    stats.checkpoint = new Checkpoint(2, 0, 0);
    stats.windowId = 3;

    stats.outputPorts = Lists.newArrayList();
    PortStats ps = new PortStats(TestGeneratorInputOperator.OUTPUT_PORT);
    ps.bufferServerBytes = 101;
    ps.tupleCount = 1;
    stats.outputPorts.add(ps);

    ohb.windowStats = Lists.newArrayList(stats);
    cstats.operators.add(ohb);
    scm.processHeartbeat(hb); // activate operator

    Assert.assertEquals(PTContainer.State.ACTIVE, o1p1.getContainer().getState());
    Assert.assertEquals("state " + o1p1, PTOperator.State.ACTIVE, o1p1.getState());

    Assert.assertEquals("tuples " + o1p1, 1, o1p1.stats.totalTuplesEmitted.get());
    Assert.assertEquals("tuples " + o1p1, 0, o1p1.stats.totalTuplesProcessed.get());
    Assert.assertEquals("window " + o1p1, 3, o1p1.stats.currentWindowId.get());

    Assert.assertEquals("port stats", 1, o1p1.stats.outputPortStatusList.size());
    PortStatus o1p1ps = o1p1.stats.outputPortStatusList.get(TestGeneratorInputOperator.OUTPUT_PORT);
    Assert.assertNotNull("port stats", o1p1ps);
    Assert.assertEquals("port stats", 1, o1p1ps.totalTuples);

    // second operator heartbeat
    stats = new OperatorStats();
    stats.checkpoint = new Checkpoint(2, 0, 0);
    stats.windowId = 4;

    stats.outputPorts = Lists.newArrayList();
    ps = new PortStats(TestGeneratorInputOperator.OUTPUT_PORT);
    ps.bufferServerBytes = 1;
    ps.tupleCount = 1;
    stats.outputPorts.add(ps);

    ohb.windowStats = Lists.newArrayList(stats);
    cstats.operators.clear();
    cstats.operators.add(ohb);
    scm.processHeartbeat(hb);

    Assert.assertEquals("tuples " + o1p1, 2, o1p1.stats.totalTuplesEmitted.get());
    Assert.assertEquals("window " + o1p1, 4, o1p1.stats.currentWindowId.get());
    Assert.assertEquals("statsQueue " + o1p1, 2, o1p1.stats.listenerStats.size());

    scm.processEvents();
    Assert.assertEquals("statsQueue " + o1p1, 0, o1p1.stats.listenerStats.size());
    Assert.assertEquals("lastStats " + o1p1, 2, o1p1.stats.lastWindowedStats.size());

  }

  public static class TestStaticPartitioningSerDe extends DefaultStatefulStreamCodec<Object>
  {

    public static final int[] partitions = new int[]{0, 1, 2};

    @Override
    public int getPartition(Object o)
    {
      if (o instanceof Tuple) {
        throw new UnsupportedOperationException("should not be called with control tuple");
      }
      return partitions[0];
    }

  }

  private int countState(PTContainer c, PTOperator.State state)
  {
    int count = 0;
    for (PTOperator o : c.getOperators()) {
      if (o.getState() == state) {
        count++;
      }
    }
    return count;
  }

  private boolean containsNodeContext(List<OperatorDeployInfo> di, OperatorMeta nodeConf)
  {
    return getNodeDeployInfo(di, nodeConf) != null;
  }

  public static List<OperatorDeployInfo> getDeployInfo(StreamingContainerAgent sca)
  {
    return sca.getDeployInfoList(sca.container.getOperators());
  }

  private static OperatorDeployInfo getNodeDeployInfo(List<OperatorDeployInfo> di, OperatorMeta nodeConf)
  {
    for (OperatorDeployInfo ndi : di) {
      if (nodeConf.getName().equals(ndi.name)) {
        return ndi;
      }
    }
    return null;
  }

  private static InputDeployInfo getInputDeployInfo(OperatorDeployInfo ndi, String streamId)
  {
    for (InputDeployInfo in : ndi.inputs) {
      if (streamId.equals(in.declaredStreamId)) {
        return in;
      }
    }
    return null;
  }

  public static StreamingContainerAgent assignContainer(StreamingContainerManager scm, String containerId)
  {
    return scm.assignContainer(new ContainerResource(0, containerId, "localhost", 1024, 0, null), InetSocketAddress.createUnresolved(containerId + "Host", 0));
  }

  @Test
  public void testValidGenericOperatorDeployInfoType()
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    TestGeneratorInputOperator.ValidGenericOperator o2 = dag.addOperator("o2", TestGeneratorInputOperator.ValidGenericOperator.class);

    dag.addStream("stream1", o1.outport1, o2.input);

    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());
    StreamingContainerManager scm = new StreamingContainerManager(dag);

    PhysicalPlan physicalPlan = scm.getPhysicalPlan();
    List<PTContainer> containers = physicalPlan.getContainers();
    for (int i = 0; i < containers.size(); ++i) {
      assignContainer(scm, "container" + (i + 1));
    }
    OperatorMeta o2Meta = dag.getMeta(o2);
    PTOperator o2Physical = physicalPlan.getOperators(o2Meta).get(0);

    String containerId = o2Physical.getContainer().getExternalId();

    OperatorDeployInfo o1DeployInfo = getDeployInfo(scm.getContainerAgent(containerId)).get(0);
    Assert.assertEquals("type " + o1DeployInfo, OperatorDeployInfo.OperatorType.GENERIC, o1DeployInfo.type);
  }

  @Test
  public void testValidInputOperatorDeployInfoType()
  {
    TestGeneratorInputOperator.ValidInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.ValidInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.addStream("stream1", o1.outport, o2.inport1);

    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());
    StreamingContainerManager scm = new StreamingContainerManager(dag);

    PhysicalPlan physicalPlan = scm.getPhysicalPlan();
    List<PTContainer> containers = physicalPlan.getContainers();
    for (int i = 0; i < containers.size(); ++i) {
      assignContainer(scm, "container" + (i + 1));
    }
    OperatorMeta o1Meta = dag.getMeta(o1);
    PTOperator o1Physical = physicalPlan.getOperators(o1Meta).get(0);

    String containerId = o1Physical.getContainer().getExternalId();

    OperatorDeployInfo o1DeployInfo = getDeployInfo(scm.getContainerAgent(containerId)).get(0);
    Assert.assertEquals("type " + o1DeployInfo, OperatorDeployInfo.OperatorType.INPUT, o1DeployInfo.type);
  }

  @Test
  public void testOperatorShutdown()
  {
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("stream1", o1.outport1, o2.inport1);
    dag.addStream("stream2", o2.outport1, o3.inport1);

    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    StreamingContainerManager scm = new StreamingContainerManager(dag);

    PhysicalPlan physicalPlan = scm.getPhysicalPlan();
    Map<PTContainer, MockContainer> mockContainers = new HashMap<>();
    for (PTContainer c : physicalPlan.getContainers()) {
      MockContainer mc = new MockContainer(scm, c);
      mockContainers.put(c, mc);
    }

    // deploy all containers
    for (Map.Entry<PTContainer, MockContainer> ce : mockContainers.entrySet()) {
      ce.getValue().deploy();
    }
    for (Map.Entry<PTContainer, MockContainer> ce : mockContainers.entrySet()) {
      // skip buffer server purge in monitorHeartbeat
      ce.getKey().bufferServerAddress = null;
    }

    List<PTOperator> o1p = physicalPlan.getOperators(dag.getMeta(o1));
    Assert.assertEquals("o1 partitions", 1,  o1p.size());
    PTOperator o1p1 = o1p.get(0);
    MockContainer mc1 = mockContainers.get(o1p1.getContainer());
    MockOperatorStats o1p1mos = mc1.stats(o1p1.getId());
    o1p1mos.currentWindowId(1).checkpointWindowId(1).deployState(DeployState.ACTIVE);
    mc1.sendHeartbeat();

    PTOperator o2p1 = physicalPlan.getOperators(dag.getMeta(o2)).get(0);
    MockContainer mc2 = mockContainers.get(o2p1.getContainer());
    MockOperatorStats o2p1mos = mc2.stats(o2p1.getId());
    o2p1mos.currentWindowId(1).checkpointWindowId(1).deployState(DeployState.ACTIVE);
    mc2.sendHeartbeat();

    Assert.assertEquals("o2 partitions", 2, physicalPlan.getOperators(dag.getMeta(o2)).size());

    PTOperator o2p2 = physicalPlan.getOperators(dag.getMeta(o2)).get(1);
    MockContainer mc3 = mockContainers.get(o2p2.getContainer());
    MockOperatorStats o2p2mos = mc3.stats(o2p2.getId());
    o2p2mos.currentWindowId(1).checkpointWindowId(1).deployState(DeployState.ACTIVE);
    mc3.sendHeartbeat();

    Assert.assertEquals("o3 partitions", 1, physicalPlan.getOperators(dag.getMeta(o3)).size());
    PTOperator o3p1 = physicalPlan.getOperators(dag.getMeta(o3)).get(0);
    MockContainer mc4 = mockContainers.get(o3p1.getContainer());
    MockOperatorStats o3p1mos = mc4.stats(o3p1.getId());

    MockOperatorStats unifierp1mos = mc4.stats(o3p1.upstreamMerge.values().iterator().next().getId());
    unifierp1mos.currentWindowId(1).checkpointWindowId(1).deployState(DeployState.ACTIVE);

    o3p1mos.currentWindowId(1).checkpointWindowId(1).deployState(DeployState.ACTIVE);
    mc4.sendHeartbeat();

    o1p1mos.currentWindowId(2).deployState(DeployState.SHUTDOWN);
    mc1.sendHeartbeat();
    o1p = physicalPlan.getOperators(dag.getMeta(o1));
    Assert.assertEquals("o1 partitions", 1,  o1p.size());
    Assert.assertEquals("o1p1 present", o1p1, o1p.get(0));
    Assert.assertEquals("input operator state", PTOperator.State.INACTIVE, o1p1.getState());
    scm.monitorHeartbeat(false);
    Assert.assertEquals("committedWindowId", -1, scm.getCommittedWindowId());
    scm.monitorHeartbeat(false); // committedWindowId updated in next cycle
    Assert.assertEquals("committedWindowId", 1, scm.getCommittedWindowId());
    scm.processEvents();
    Assert.assertEquals("containers at committedWindowId=1", 4, physicalPlan.getContainers().size());

    // checkpoint window 2
    o1p1mos.checkpointWindowId(2);
    mc1.sendHeartbeat();
    scm.monitorHeartbeat(false);

    Assert.assertEquals("committedWindowId", 1, scm.getCommittedWindowId());

    o2p1mos.currentWindowId(2).checkpointWindowId(2);
    o2p2mos.currentWindowId(2).checkpointWindowId(2);
    o3p1mos.currentWindowId(2).checkpointWindowId(2);
    unifierp1mos.currentWindowId(2).checkpointWindowId(2);

    mc2.sendHeartbeat();
    mc3.sendHeartbeat();
    mc4.sendHeartbeat();
    scm.monitorHeartbeat(false);

    // Operators are shutdown when both operators reach window Id 2
    Assert.assertEquals(0, o1p1.getContainer().getOperators().size());
    Assert.assertEquals(0, o2p1.getContainer().getOperators().size());
    Assert.assertEquals(0, physicalPlan.getContainers().size());
  }

  private void testDownStreamPartition(Locality locality) throws Exception
  {
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));
    dag.addStream("o1Output1", o1.outport, o2.inport1).setLocality(locality);

    int maxContainers = 5;
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, maxContainers);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());
    dag.validate();
    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    Assert.assertEquals("number of containers", 1, plan.getContainers().size());

    PTContainer container1 = plan.getContainers().get(0);
    Assert.assertEquals("number operators " + container1, 3, container1.getOperators().size());
    StramLocalCluster slc = new StramLocalCluster(dag);
    slc.run(5000);
  }

  @Test
  public void testOIODownstreamPartition() throws Exception
  {
    testDownStreamPartition(Locality.THREAD_LOCAL);
  }

  @Test
  public void testContainerLocalDownstreamPartition() throws Exception
  {
    testDownStreamPartition(Locality.CONTAINER_LOCAL);
  }

  @Test
  public void testPhysicalPropertyUpdate() throws Exception
  {
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.addStream("o1.outport", o1.outport, o2.inport1);
    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.runAsync();
    StreamingContainerManager dnmgr = lc.dnmgr;
    Map<Integer,PTOperator> operatorMap = dnmgr.getPhysicalPlan().getAllOperators();
    for (PTOperator p: operatorMap.values()) {
      StramTestSupport.waitForActivation(lc, p);
    }
    dnmgr.setPhysicalOperatorProperty(lc.getPlanOperators(dag.getMeta(o1)).get(0).getId(),"maxTuples","2");
    Future<?> future = dnmgr.getPhysicalOperatorProperty(lc.getPlanOperators(dag.getMeta(o1)).get(0).getId(), "maxTuples", 10000);
    Object object = future.get(10000, TimeUnit.MILLISECONDS);
    Assert.assertNotNull(object);
    @SuppressWarnings("unchecked")
    Map<String, Object> propertyValue = (Map<String, Object>)object;
    Assert.assertEquals(2,propertyValue.get("maxTuples"));
    lc.shutdown();
  }

  private void setupAppDataSourceLogicalPlan(Class<? extends TestAppDataQueryOperator> qClass,
      Class<? extends TestAppDataSourceOperator> dsClass, Class<? extends TestAppDataResultOperator> rClass)
  {
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    TestAppDataQueryOperator q = dag.addOperator("q", qClass);
    TestAppDataResultOperator r = dag.addOperator("r", rClass);
    TestAppDataSourceOperator ds = dag.addOperator("ds", dsClass);

    q.setAppDataUrl("ws://123.123.123.123:9090/pubsub");
    q.setTopic("xyz.query");
    r.setAppDataUrl("ws://123.123.123.124:9090/pubsub");
    r.setTopic("xyz.result");

    dag.addStream("o1-to-ds", o1.outport, ds.inport1);
    dag.addStream("q-to-ds", q.outport, ds.query);
    dag.addStream("ds-to-r", ds.result, r.inport);
  }

  private void testAppDataSources(boolean appendQIDToTopic) throws Exception
  {
    StramLocalCluster lc = new StramLocalCluster(dag);
    StreamingContainerManager dnmgr = lc.dnmgr;
    List<AppDataSource> appDataSources = dnmgr.getAppDataSources();
    Assert.assertEquals("There should be exactly one data source", 1, appDataSources.size());
    AppDataSource ads = appDataSources.get(0);
    Assert.assertEquals("Data Source name verification", "ds.result", ads.getName());
    AppDataSource.QueryInfo query = ads.getQuery();
    Assert.assertEquals("Query operator name verification", "q", query.operatorName);
    Assert.assertEquals("Query topic verification", "xyz.query", query.topic);
    Assert.assertEquals("Query URL verification", "ws://123.123.123.123:9090/pubsub", query.url);
    AppDataSource.ResultInfo result = ads.getResult();
    Assert.assertEquals("Result operator name verification", "r", result.operatorName);
    Assert.assertEquals("Result topic verification", "xyz.result", result.topic);
    Assert.assertEquals("Result URL verification", "ws://123.123.123.124:9090/pubsub", result.url);
    Assert.assertEquals("Result QID append verification", appendQIDToTopic, result.appendQIDToTopic);
  }

  @Test
  public void testGetAppDataSources1() throws Exception
  {
    setupAppDataSourceLogicalPlan(TestAppDataQueryOperator.class, TestAppDataSourceOperator.class, TestAppDataResultOperator.ResultOperator1.class);
    testAppDataSources(true);
  }

  @Test
  public void testGetAppDataSources2() throws Exception
  {
    setupAppDataSourceLogicalPlan(TestAppDataQueryOperator.class, TestAppDataSourceOperator.class, TestAppDataResultOperator.ResultOperator2.class);
    testAppDataSources(false);
  }

  @Test
  public void testGetAppDataSources3() throws Exception
  {
    setupAppDataSourceLogicalPlan(TestAppDataQueryOperator.class, TestAppDataSourceOperator.class, TestAppDataResultOperator.ResultOperator3.class);
    testAppDataSources(false);
  }

  @Test
  public void testAppDataPush() throws Exception
  {
    if (StramTestSupport.isInTravis()) {
      // disable this test in travis because of an intermittent problem similar to this:
      // http://stackoverflow.com/questions/32172925/travis-ci-sporadic-timeouts-to-localhost
      // We should remove this when we find a solution to this.
      LOG.info("Test testAppDataPush is disabled in Travis");
      return;
    }
    final String topic = "xyz";
    final List<String> messages = new ArrayList<>();
    EmbeddedWebSocketServer server = new EmbeddedWebSocketServer(0);
    server.setWebSocket(new WebSocket.OnTextMessage()
    {

      @Override
      public void onMessage(String data)
      {
        messages.add(data);
      }

      @Override
      public void onOpen(WebSocket.Connection connection)
      {
      }

      @Override
      public void onClose(int closeCode, String message)
      {
      }
    });
    try {
      server.start();
      int port = server.getPort();
      TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
      GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
      dag.addStream("o1.outport", o1.outport, o2.inport1);
      dag.setAttribute(LogicalPlan.METRICS_TRANSPORT, new AutoMetricBuiltInTransport(topic));
      dag.setAttribute(LogicalPlan.GATEWAY_CONNECT_ADDRESS, "localhost:" + port);
      dag.setAttribute(LogicalPlan.PUBSUB_CONNECT_TIMEOUT_MILLIS, 2000);

      StramLocalCluster lc = new StramLocalCluster(dag);
      StreamingContainerManager dnmgr = lc.dnmgr;
      StramAppContext appContext = new StramTestSupport.TestAppContext(dag.getAttributes());

      AppDataPushAgent pushAgent = new AppDataPushAgent(dnmgr, appContext);
      pushAgent.init();
      pushAgent.pushData();
      Thread.sleep(1000);
      Assert.assertTrue(messages.size() > 0);
      pushAgent.close();
      JSONObject message = new JSONObject(messages.get(0));
      Assert.assertEquals(topic, message.getString("topic"));
      Assert.assertEquals("publish", message.getString("type"));
      JSONObject data = message.getJSONObject("data");
      Assert.assertTrue(StringUtils.isNotBlank(data.getString("appId")));
      Assert.assertTrue(StringUtils.isNotBlank(data.getString("appUser")));
      Assert.assertTrue(StringUtils.isNotBlank(data.getString("appName")));

      JSONObject logicalOperators = data.getJSONObject("logicalOperators");
      for (String opName : new String[]{"o1", "o2"}) {
        JSONObject opObj = logicalOperators.getJSONObject(opName);
        Assert.assertTrue(opObj.has("totalTuplesProcessed"));
        Assert.assertTrue(opObj.has("totalTuplesEmitted"));
        Assert.assertTrue(opObj.has("tuplesProcessedPSMA"));
        Assert.assertTrue(opObj.has("tuplesEmittedPSMA"));
        Assert.assertTrue(opObj.has("latencyMA"));
      }
    } finally {
      server.stop();
    }
  }

  public static class TestMetricTransport implements AutoMetric.Transport, Serializable
  {
    private String prefix;
    private static List<String> messages = new ArrayList<>();

    public TestMetricTransport(String prefix)
    {
      this.prefix = prefix;
    }

    @Override
    public void push(String jsonData) throws IOException
    {
      messages.add(prefix + ":" + jsonData);
    }

    @Override
    public long getSchemaResendInterval()
    {
      return 0;
    }
  }

  @Test
  public void testCustomMetricsTransport() throws Exception
  {
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.addStream("o1.outport", o1.outport, o2.inport1);
    dag.setAttribute(LogicalPlan.METRICS_TRANSPORT, new TestMetricTransport("xyz"));
    StramLocalCluster lc = new StramLocalCluster(dag);
    StreamingContainerManager dnmgr = lc.dnmgr;
    StramAppContext appContext = new StramTestSupport.TestAppContext(dag.getAttributes());

    AppDataPushAgent pushAgent = new AppDataPushAgent(dnmgr, appContext);
    pushAgent.init();
    pushAgent.pushData();
    Assert.assertTrue(TestMetricTransport.messages.size() > 0);
    pushAgent.close();
    String msg = TestMetricTransport.messages.get(0);
    Assert.assertTrue(msg.startsWith("xyz:"));
  }

  public static class HighLatencyTestOperator extends GenericTestOperator
  {
    private long latency;

    @Override
    public void endWindow()
    {
      try {
        Thread.sleep(latency);
      } catch (InterruptedException ex) {
        // move on
      }
    }

    public void setLatency(long latency)
    {
      this.latency = latency;
    }

  }

  @Test
  public void testLatency() throws Exception
  {
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    HighLatencyTestOperator o3 = dag.addOperator("o3", HighLatencyTestOperator.class);
    GenericTestOperator o4 = dag.addOperator("o4", GenericTestOperator.class);
    long latency = 5000; // 5 seconds
    o3.setLatency(latency);
    dag.addStream("o1.outport", o1.outport, o2.inport1, o3.inport1);
    dag.addStream("o2.outport1", o2.outport1, o4.inport1);
    dag.addStream("o3.outport1", o3.outport1, o4.inport2);
    dag.setAttribute(Context.DAGContext.STATS_MAX_ALLOWABLE_WINDOWS_LAG, 2); // 1 second
    StramLocalCluster lc = new StramLocalCluster(dag);
    StreamingContainerManager dnmgr = lc.dnmgr;
    lc.runAsync();
    Thread.sleep(10000);
    LogicalOperatorInfo o1Info = dnmgr.getLogicalOperatorInfo("o1");
    LogicalOperatorInfo o2Info = dnmgr.getLogicalOperatorInfo("o2");
    LogicalOperatorInfo o3Info = dnmgr.getLogicalOperatorInfo("o3");
    LogicalOperatorInfo o4Info = dnmgr.getLogicalOperatorInfo("o4");

    Assert.assertEquals("Input operator latency must be zero", 0, o1Info.latencyMA);
    Assert.assertTrue("Latency must be greater than or equal to zero", o2Info.latencyMA >= 0);
    Assert.assertTrue("Actual latency must be greater than the artificially introduced latency",
        o3Info.latencyMA > latency);
    Assert.assertTrue("Latency must be greater than or equal to zero", o4Info.latencyMA >= 0);
    StreamingContainerManager.CriticalPathInfo criticalPathInfo = dnmgr.getCriticalPathInfo();
    Assert.assertArrayEquals("Critical Path must be the path in the DAG that includes the HighLatencyTestOperator",
        new Integer[]{o1Info.partitions.iterator().next(), o3Info.partitions.iterator().next(), o4Info.partitions.iterator().next()},
        criticalPathInfo.path.toArray());
    Assert.assertTrue("Whole DAG latency must be greater than the artificially introduced latency",
        criticalPathInfo.latency > latency);
    lc.shutdown();
  }

  private static final Logger LOG = LoggerFactory.getLogger(StreamingContainerManagerTest.class);
}

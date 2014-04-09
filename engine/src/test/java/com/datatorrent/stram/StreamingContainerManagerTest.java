/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.Stats.OperatorStats.PortStats;
import com.datatorrent.api.StatsListener;

import com.datatorrent.stram.StramChildAgent.ContainerStartRequest;
import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.api.OperatorDeployInfo.InputDeployInfo;
import com.datatorrent.stram.api.OperatorDeployInfo.OutputDeployInfo;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;
import com.datatorrent.stram.engine.DefaultUnifier;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.physical.OperatorStatus.PortStatus;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.plan.physical.PhysicalPlanTest;
import com.datatorrent.stram.plan.physical.PhysicalPlanTest.PartitioningTestOperator;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;
import com.datatorrent.stram.tuple.Tuple;

public class StreamingContainerManagerTest {
  @Rule public TestMeta testMeta = new TestMeta();

  @Test
  public void testDeployInfoSerialization() throws Exception {
    OperatorDeployInfo ndi = new OperatorDeployInfo();
    ndi.name = "node1";
    ndi.type = OperatorDeployInfo.OperatorType.GENERIC;
    ndi.id = 1;
    ndi.contextAttributes = new AttributeMap.DefaultAttributeMap();
    ndi.contextAttributes.put(OperatorContext.SPIN_MILLIS, 100);

    OperatorDeployInfo.InputDeployInfo input = new OperatorDeployInfo.InputDeployInfo();
    input.declaredStreamId = "streamToNode";
    input.portName = "inputPortNameOnNode";
    input.sourceNodeId = 99;

    ndi.inputs = new ArrayList<OperatorDeployInfo.InputDeployInfo>();
    ndi.inputs.add(input);

    OperatorDeployInfo.OutputDeployInfo output = new OperatorDeployInfo.OutputDeployInfo();
    output.declaredStreamId = "streamFromNode";
    output.portName = "outputPortNameOnNode";

    ndi.outputs = new ArrayList<OperatorDeployInfo.OutputDeployInfo>();
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
  public void testGenerateDeployInfo() {

    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    GenericTestOperator o4 = dag.addOperator("o4", GenericTestOperator.class);

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

    dnm.assignContainer(new ContainerResource(0, "container1Id", "host1", 1024, null), InetSocketAddress.createUnresolved("host1", 9001));
    dnm.assignContainer(new ContainerResource(0, "container2Id", "host2", 1024, null), InetSocketAddress.createUnresolved("host2", 9002));

    StramChildAgent sca1 = dnm.getContainerAgent(dnm.getPhysicalPlan().getContainers().get(0).getExternalId());
    StramChildAgent sca2 = dnm.getContainerAgent(dnm.getPhysicalPlan().getContainers().get(1).getExternalId());

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
    Assert.assertNotNull(o2.getName() + " assigned to " + sca2.container.getExternalId(), o2DI);
    Assert.assertNotNull(o3.getName() + " assigned to " + sca2.container.getExternalId(), o3DI);

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
    Assert.assertNotNull(o4.getName() + " assigned to " + sca2.container.getExternalId(), o4DI);
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
  public void testStaticPartitioning() {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    PhysicalPlanTest.PartitioningTestOperator node2 = dag.addOperator("node2", PhysicalPlanTest.PartitioningTestOperator.class);
    dag.setAttribute(node2, OperatorContext.INITIAL_PARTITION_COUNT, 3);
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

    Assert.assertEquals("number containers", 6, plan.getContainers().size());
    List<StramChildAgent> containerAgents = Lists.newArrayList();
    for (int i=0; i < plan.getContainers().size(); i++) {
      containerAgents.add(assignContainer(dnm, "container"+(i+1)));
    }

    PTContainer c = plan.getOperators(dag.getMeta(node1)).get(0).getContainer();
    StramChildAgent sca1 = dnm.getContainerAgent(c.getExternalId());
    List<OperatorDeployInfo> c1 = getDeployInfo(sca1);
    Assert.assertEquals("number operators assigned to container", 1, c1.size());
    Assert.assertTrue(node2.getName() + " assigned to " + sca1.container.getExternalId(), containsNodeContext(c1, dag.getMeta(node1)));

    List<PTOperator> o2Partitions = plan.getOperators(dag.getMeta(node2));
    Assert.assertEquals("number partitions", TestStaticPartitioningSerDe.partitions.length, o2Partitions.size());

    for (int i=0; i<o2Partitions.size(); i++) {
      String containerId = o2Partitions.get(i).getContainer().getExternalId();
      List<OperatorDeployInfo> cc = getDeployInfo(dnm.getContainerAgent(containerId));
      Assert.assertEquals("number operators assigned to container", 1, cc.size());
      Assert.assertTrue(node2.getName() + " assigned to " + containerId, containsNodeContext(cc, dag.getMeta(node2)));

      // n1n2 in, mergeStream out
      OperatorDeployInfo ndi = cc.get(0);
      Assert.assertEquals("type " + ndi, OperatorDeployInfo.OperatorType.GENERIC, ndi.type);
      Assert.assertEquals("inputs " + ndi, 1, ndi.inputs.size());
      Assert.assertEquals("outputs " + ndi, 1, ndi.outputs.size());

      InputDeployInfo nidi = ndi.inputs.get(0);
      Assert.assertEquals("stream " + nidi, n1n2.getName(), nidi.declaredStreamId);
      Assert.assertEquals("partition for " + containerId, Sets.newHashSet(PartitioningTestOperator.PARTITION_KEYS[i]), nidi.partitionKeys);
      Assert.assertEquals("serde " + nidi, null, nidi.serDeClassName);
    }

    // unifier
    List<PTOperator> o2Unifiers = plan.getMergeOperators(dag.getMeta(node2));
    Assert.assertEquals("number unifiers", 1, o2Unifiers.size());
    List<OperatorDeployInfo> cUnifier = getDeployInfo(dnm.getContainerAgent(o2Unifiers.get(0).getContainer().getExternalId()));
    Assert.assertEquals("number operators " + cUnifier, 1, cUnifier.size());

    OperatorDeployInfo mergeNodeDI = getNodeDeployInfo(cUnifier,  dag.getMeta(node2));
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
      Assert.assertEquals("contextAttributes " , new Integer(1111), nidi.getValue(PortContext.QUEUE_CAPACITY));
      sourceNodeIds.add(nidi.sourceNodeId);
    }
    for (PTOperator node : dnm.getPhysicalPlan().getOperators(dag.getMeta(node2))) {
      Assert.assertTrue(sourceNodeIds + " contains " + node.getId(), sourceNodeIds.contains(node.getId()));
    }

    Assert.assertEquals("outputs " + mergeNodeDI, 1, mergeNodeDI.outputs.size());
    for (OutputDeployInfo odi : mergeNodeDI.outputs) {
      Assert.assertNotNull("contextAttributes " + odi, odi.contextAttributes);
      Assert.assertEquals("contextAttributes " , new Integer(2222), odi.getValue(PortContext.QUEUE_CAPACITY));
    }

    try {
      Object operator = msa.load(mergeNodeDI.id, Checkpoint.STATELESS_CHECKPOINT_WINDOW_ID);
      Assert.assertTrue("" + operator,  operator instanceof DefaultUnifier);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    // node3 container
    c = plan.getOperators(dag.getMeta(node3)).get(0).getContainer();
    List<OperatorDeployInfo> cmerge = getDeployInfo(dnm.getContainerAgent(c.getExternalId()));
    Assert.assertEquals("number operators " + cmerge, 1, cmerge.size());

    OperatorDeployInfo node3DI = getNodeDeployInfo(cmerge,  dag.getMeta(node3));
    Assert.assertNotNull(node3.getName() + " assigned", node3DI);
    Assert.assertEquals("inputs " + node3DI, 1, node3DI.inputs.size());
    InputDeployInfo node3In = node3DI.inputs.get(0);
    Assert.assertEquals("streamName " + node3In, n2n3.getName(), node3In.declaredStreamId);
    Assert.assertEquals("portName " + node3In, dag.getMeta(node3).getMeta(node3.inport1).getPortName(), node3In.portName);
    Assert.assertNotNull("sourceNodeId " + node3DI, node3In.sourceNodeId);
    Assert.assertEquals("sourcePortName " + node3DI, mergeNodeDI.outputs.get(0).portName, node3In.sourcePortName);
  }

  @Test
  public void testPhysicalPropertyUpdate() {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);

    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.setAttribute(o1, OperatorContext.INITIAL_PARTITION_COUNT, 3);
    dag.addStream("o1.outport", o1.outport, o2.inport1);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();
    List<PTOperator> o1Partitions = plan.getOperators(dag.getMeta(o1));
    List<StramChildAgent> containerAgents = Lists.newArrayList();
    for (int i=0; i < plan.getContainers().size(); i++) {
      containerAgents.add(assignContainer(dnm, "container"+(i+1)));
    }

    Assert.assertEquals("number of partitions", 3,o1Partitions.size());
    PTOperator o = o1Partitions.get(0);
    Map<String,Object> m = dnm.getPhysicalOperatorProperty(o.getId()+"");
    int origionalValue = ((Integer)m.get("maxTuples")).intValue();

    dnm.setPhysicalOperatorProperty(o.getId()+"", "maxTuples","2" );
    m = dnm.getPhysicalOperatorProperty(o.getId()+"");
    int newVal = Integer.valueOf(m.get("maxTuples").toString());
    Assert.assertEquals(2,newVal);
    for(int i = 1; i< 3;i++){
      o = o1Partitions.get(i);
      m = dnm.getPhysicalOperatorProperty(o.getId()+"");
      Assert.assertEquals(origionalValue,((Integer)m.get("maxTuples")).intValue());
    }

  }

  @Test
  public void testRecoveryOrder() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);
    GenericTestOperator node3 = dag.addOperator("node3", GenericTestOperator.class);

    dag.addStream("n1n2", node1.outport1, node2.inport1);
    dag.addStream("n2n3", node2.outport1, node3.inport1);

    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    Assert.assertEquals(""+scm.containerStartRequests, 2, scm.containerStartRequests.size());
    scm.containerStartRequests.clear();

    PhysicalPlan plan = scm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals(""+containers, 2, plan.getContainers().size());

    PTContainer c1 = containers.get(0);
    Assert.assertEquals("c1.operators "+c1.getOperators(), 2, c1.getOperators().size());

    PTContainer c2 = containers.get(1);
    Assert.assertEquals("c2.operators "+c2.getOperators(), 1, c2.getOperators().size());

    assignContainer(scm, "container1");
    assignContainer(scm, "container2");

    StramChildAgent sca1 = scm.getContainerAgent(c1.getExternalId());
    StramChildAgent sca2 = scm.getContainerAgent(c2.getExternalId());
    Assert.assertEquals("", 0, countState(sca1.container, PTOperator.State.PENDING_UNDEPLOY));
    Assert.assertEquals("", 2, countState(sca1.container, PTOperator.State.PENDING_DEPLOY));

    scm.scheduleContainerRestart(c1.getExternalId());
    Assert.assertEquals("", 0, countState(sca1.container, PTOperator.State.PENDING_UNDEPLOY));
    Assert.assertEquals("", 2, countState(sca1.container, PTOperator.State.PENDING_DEPLOY));
    Assert.assertEquals(""+scm.containerStartRequests, 1, scm.containerStartRequests.size());
    ContainerStartRequest dr = scm.containerStartRequests.peek();
    Assert.assertNotNull(dr);

    Assert.assertEquals(""+sca2.container, 1, countState(sca2.container, PTOperator.State.PENDING_UNDEPLOY));
    Assert.assertEquals(""+sca2.container, 0, countState(sca2.container, PTOperator.State.PENDING_DEPLOY));

  }

  @Test
  public void testRecoveryUpstreamInline() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);

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
  public void testCheckpointWindowIds() throws Exception {
    File path =  new File(testMeta.dir);
    FileUtils.deleteDirectory(path.getAbsoluteFile());

    FSStorageAgent sa = new FSStorageAgent(path.getPath(), null);

    long[] windowIds = new long[]{123L, 345L, 234L};
    for (long windowId : windowIds) {
      sa.save(windowId, 1, windowId);
    }

    Arrays.sort(windowIds);
    long[] windowsIds = sa.getWindowIds(1);
    Arrays.sort(windowsIds);
    Assert.assertArrayEquals("Saved windowIds", windowIds, windowsIds);

    for (long windowId : windowIds) {
      sa.delete(1, windowId);
    }
    try {
      sa.getWindowIds(1);
      Assert.fail("There should not be any most recently saved windowId!");
    }
    catch (IOException io) {
      Assert.assertTrue("No State Saved", true);
    }
  }

  @Test
  public void testProcessHeartbeat() throws Exception
  {
    FileUtils.deleteDirectory(new File(testMeta.dir)); // clean any state from previous run

    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);

    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
     dag.setAttribute(o1, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{new PartitioningTest.PartitionLoadWatch()}));
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan plan = scm.getPhysicalPlan();
    Assert.assertEquals("number required containers", 1, plan.getContainers().size());

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);

    // assign container
    String containerId = "container1";
    StramChildAgent sca = scm.assignContainer(new ContainerResource(0, containerId, "localhost", 0, null), InetSocketAddress.createUnresolved("localhost", 0));
    Assert.assertNotNull(sca);

    Assert.assertEquals(PTContainer.State.ALLOCATED, o1p1.getContainer().getState());
    Assert.assertEquals(PTOperator.State.PENDING_DEPLOY, o1p1.getState());

    ContainerStats cstats = new ContainerStats(containerId);
    ContainerHeartbeat hb = new ContainerHeartbeat();
    hb.setContainerStats(cstats);

    ContainerHeartbeatResponse chr = scm.processHeartbeat(hb); // get deploy request
    Assert.assertNotNull(chr.deployRequest);
    Assert.assertEquals(""+chr.deployRequest, 1, chr.deployRequest.size());
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

  public static class TestStaticPartitioningSerDe extends DefaultStatefulStreamCodec<Object> {

    public final static int[] partitions = new int[]{
      0, 1, 2
    };

    @Override
    public int getPartition(Object o)
    {
      if (o instanceof Tuple) {
        throw new UnsupportedOperationException("should not be called with control tuple");
      }
      return partitions[0];
    }

  }

  private int countState(PTContainer c, PTOperator.State state) {
    int count = 0;
    for (PTOperator o : c.getOperators()) {
      if (o.getState() == state) {
        count++;
      }
    }
    return count;
  }

  private boolean containsNodeContext(List<OperatorDeployInfo> di, OperatorMeta nodeConf) {
    return getNodeDeployInfo(di, nodeConf) != null;
  }

  private static List<OperatorDeployInfo> getDeployInfo(StramChildAgent sca) {
    return sca.getDeployInfoList(sca.container.getOperators());
  }

  private static OperatorDeployInfo getNodeDeployInfo(List<OperatorDeployInfo> di, OperatorMeta nodeConf) {
    for (OperatorDeployInfo ndi : di) {
      if (nodeConf.getName().equals(ndi.name)) {
        return ndi;
      }
    }
    return null;
  }

  private static InputDeployInfo getInputDeployInfo(OperatorDeployInfo ndi, String streamId) {
    for (InputDeployInfo in : ndi.inputs) {
      if (streamId.equals(in.declaredStreamId)) {
        return in;
      }
    }
    return null;
  }

  private static StramChildAgent assignContainer(StreamingContainerManager scm, String containerId) {
    return scm.assignContainer(new ContainerResource(0, containerId, "localhost", 1024, null), InetSocketAddress.createUnresolved(containerId+"Host", 0));
  }

}

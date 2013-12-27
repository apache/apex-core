/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan.physical;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.FutureTask;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.stram.StramChildAgent;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.StramJournal;
import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.StramJournal.SetOperatorState;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.TestPlanContext;
import com.datatorrent.stram.plan.logical.CreateOperatorRequest;
import com.datatorrent.stram.plan.logical.CreateStreamRequest;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.physical.PhysicalPlanTest.PartitioningTestOperator;
import com.google.common.collect.Lists;

public class SerializationTest
{
  private static final Logger LOG = LoggerFactory.getLogger(SerializationTest.class);

  @Test
  public void testPhysicalPlan() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    PartitioningTestOperator o2 = dag.addOperator("o2", PartitioningTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1, o2.inportWithCodec);
    dag.addStream("mergeStream", o2.outport1, o3.inport1);

    dag.getMeta(o2).getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, 3);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);

    TestPlanContext ctx = new TestPlanContext();
    PhysicalPlan plan = new PhysicalPlan(dag, ctx);

    ByteArrayOutputStream  bos = new ByteArrayOutputStream();
    LogicalPlan.write(dag, bos);
    LOG.debug("logicalPlan size: " + bos.toByteArray().length);

    bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(plan);
    LOG.debug("physicalPlan size: " + bos.toByteArray().length);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    plan = (PhysicalPlan)new ObjectInputStream(bis).readObject();

    dag = plan.getDAG();

    Field f = PhysicalPlan.class.getDeclaredField("ctx");
    f.setAccessible(true);
    f.set(plan, ctx);
    f.setAccessible(false);

    OperatorMeta o2Meta = dag.getOperatorMeta("o2");
    List<PTOperator> o2Partitions = plan.getOperators(o2Meta);
    Assert.assertEquals(3, o2Partitions.size());
    for (PTOperator o : o2Partitions) {
      Assert.assertNotNull("partition null " + o, o.getPartitionKeys());
      Assert.assertEquals("partition keys " + o + " " + o.getPartitionKeys(), 2, o.getPartitionKeys().size());
      PartitioningTestOperator partitionedInstance = (PartitioningTestOperator)plan.loadOperator(o);
      Assert.assertEquals("instance per partition", o.getPartitionKeys().values().toString(), partitionedInstance.pks);
      Assert.assertNotNull("partition stats null " + o, o.stats);
    }

  }

  /**
   * Test serialization of the container manager with mocked execution layer.
   * @throws Exception
   */
  @Test
  public void testContainerManager() throws Exception
  {
    File testWorkDir = new File("target", SerializationTest.class.getName());
    FileUtils.deleteDirectory(testWorkDir); // clean any state from previous run

    LogicalPlan dag = new LogicalPlan();
    //dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 1);
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, testWorkDir.getPath());

    StreamingContainerManager scm = StreamingContainerManager.getInstance(dag, false);
    PhysicalPlan plan = scm.getPhysicalPlan();
    Assert.assertEquals("number required containers", 1, plan.getContainers().size());

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);

    // container allocation
    String containerId = "container1";
    StramChildAgent sca = scm.assignContainer(new ContainerResource(0, containerId, "localhost", 0, null), InetSocketAddress.createUnresolved("localhost", 0));
    Assert.assertNotNull(sca);

    Assert.assertEquals(PTContainer.State.ALLOCATED, o1p1.getContainer().getState());
    Assert.assertEquals(PTOperator.State.INACTIVE, o1p1.getState());

    ContainerStats cstats = new ContainerStats(containerId);
    ContainerHeartbeat hb = new ContainerHeartbeat();
    hb.setContainerStats(cstats);

    ContainerHeartbeatResponse chr = scm.processHeartbeat(hb); // get deploy request
    Assert.assertNotNull(chr.deployRequest);
    Assert.assertEquals(""+chr.deployRequest, 1, chr.deployRequest.size());
    Assert.assertEquals(PTContainer.State.ACTIVE, o1p1.getContainer().getState());
    Assert.assertEquals("state " + o1p1, PTOperator.State.PENDING_DEPLOY, o1p1.getState());

    // operator reports first stats
    OperatorHeartbeat ohb = new OperatorHeartbeat();
    ohb.setNodeId(o1p1.getId());
    ohb.setState(OperatorHeartbeat.DeployState.ACTIVE.name());
    OperatorStats stats = new OperatorStats();
    stats.checkpointedWindowId = 2;
    stats.windowId = 3;
    ohb.windowStats = Lists.newArrayList(stats);
    cstats.operators.add(ohb);
    chr = scm.processHeartbeat(hb); // get deploy request

    Assert.assertEquals(PTContainer.State.ACTIVE, o1p1.getContainer().getState());
    Assert.assertEquals("state " + o1p1, PTOperator.State.ACTIVE, o1p1.getState());

    // logical plan modification that requires checkpointing
    CreateOperatorRequest cor = new CreateOperatorRequest();
    cor.setOperatorFQCN(GenericTestOperator.class.getName());
    cor.setOperatorName("o2");
    CreateStreamRequest csr = new CreateStreamRequest();
    csr.setSourceOperatorName("o1");
    csr.setSourceOperatorPortName("outputPort");
    csr.setSinkOperatorName("o2");
    csr.setSinkOperatorPortName("input1");
    FutureTask<?> lpmf = scm.logicalPlanModification(Lists.newArrayList(cor, csr));
    while (!lpmf.isDone()) {
      scm.monitorHeartbeat();
    }
    Assert.assertNull(lpmf.get()); // unmask exception, if any

    Assert.assertSame("dag references", dag, scm.getLogicalPlan());
    Assert.assertEquals("number operators after plan modification", 2, dag.getAllOperators().size());

    // test plan restore
    dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, testWorkDir.getPath());
    scm = StreamingContainerManager.getInstance(dag, false);
    Assert.assertNotSame("dag references", dag, scm.getLogicalPlan());
    Assert.assertEquals("number operators after restore", 2, scm.getLogicalPlan().getAllOperators().size());

  }

  @Test
  public void testWriteAheadLog() throws Exception
  {
    final MutableInt flushCount = new MutableInt();
    LogicalPlan dag = new LogicalPlan();
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan plan = scm.getPhysicalPlan();

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);
    Assert.assertEquals(PTOperator.State.INACTIVE, o1p1.getState());

    ByteArrayOutputStream bos = new ByteArrayOutputStream() {
      @Override
      public void flush() throws IOException {
        super.flush();
        flushCount.increment();
      }
    };
    StramJournal j = new StramJournal(new DataOutputStream(bos));
    SetOperatorState op1 = new SetOperatorState(scm);
    j.register(1, op1);

    op1 = new SetOperatorState(scm);
    op1.operatorId = o1p1.getId();
    op1.state = PTOperator.State.ACTIVE;
    j.writeLog(op1);
    Assert.assertEquals("flush count", 1, flushCount.intValue());

    Assert.assertEquals(PTOperator.State.INACTIVE, o1p1.getState());
    bos.close();

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    j.replay(new DataInputStream(bis));
    Assert.assertEquals(PTOperator.State.ACTIVE, o1p1.getState());

  }

}

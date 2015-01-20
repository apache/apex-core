/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.*;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.MockitoUtil;

import com.datatorrent.lib.util.FSStorageAgent;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StorageAgent;

import com.datatorrent.stram.Journal.SetContainerState;
import com.datatorrent.stram.Journal.SetOperatorState;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.TestPlanContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.requests.CreateOperatorRequest;
import com.datatorrent.stram.plan.logical.requests.CreateStreamRequest;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.plan.physical.PhysicalPlanTest.PartitioningTestOperator;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;

public class StramRecoveryTest
{
  private static final Logger LOG = LoggerFactory.getLogger(StramRecoveryTest.class);
  @Rule public final TestMeta testMeta = new TestMeta();

  @Test
  public void testPhysicalPlanSerialization() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    PartitioningTestOperator o2 = dag.addOperator("o2", PartitioningTestOperator.class);
    o2.setPartitionCount(3);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1, o2.inportWithCodec);
    dag.addStream("mergeStream", o2.outport1, o3.inport1);

    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new FSStorageAgent(testMeta.dir, null));
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

    dag = plan.getLogicalPlan();

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

  public static class StatsListeningOperator extends TestGeneratorInputOperator implements StatsListener
  {
    int processStatsCnt = 0;
    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      processStatsCnt++;
      return null;
    }
  }

  public static void checkpoint(StreamingContainerManager scm, PTOperator oper, Checkpoint checkpoint) throws Exception
  {
    // write checkpoint while AM is out,
    // it needs to be picked up as part of restore
    StorageAgent sa = oper.getOperatorMeta().getValue(OperatorContext.STORAGE_AGENT);
    sa.save(oper.getOperatorMeta().getOperator(), oper.getId(), checkpoint.windowId);
  }

  /**
   * Test serialization of the container manager with mock execution layer.
   * @throws Exception
   */
  @Test
  public void testContainerManager() throws Exception
  {
    FileUtils.deleteDirectory(new File(testMeta.dir)); // clean any state from previous run

    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new FSStorageAgent(testMeta.dir, null));

    StatsListeningOperator o1 = dag.addOperator("o1", StatsListeningOperator.class);

    FSRecoveryHandler recoveryHandler = new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false));
    StreamingContainerManager scm = StreamingContainerManager.getInstance(recoveryHandler, dag, false);
    File expFile = new File(recoveryHandler.getDir(), FSRecoveryHandler.FILE_SNAPSHOT);
    Assert.assertTrue("snapshot file " + expFile, expFile.exists());

    PhysicalPlan plan = scm.getPhysicalPlan();
    Assert.assertEquals("number required containers", 1, plan.getContainers().size());

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);

    @SuppressWarnings("UnusedAssignment") /* sneaky: the constructor does some changes to the container */
    MockContainer mc = new MockContainer(scm, o1p1.getContainer());
    PTContainer originalContainer = o1p1.getContainer();

    Assert.assertNotNull(o1p1.getContainer().bufferServerAddress);
    Assert.assertEquals(PTContainer.State.ACTIVE, o1p1.getContainer().getState());
    Assert.assertEquals("state " + o1p1, PTOperator.State.PENDING_DEPLOY, o1p1.getState());

    // test restore initial snapshot + log
    dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);
    scm = StreamingContainerManager.getInstance(new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false)), dag, false);
    dag = scm.getLogicalPlan();
    plan = scm.getPhysicalPlan();

    o1p1 = plan.getOperators(dag.getOperatorMeta("o1")).get(0);
    Assert.assertEquals("post restore state " + o1p1, PTOperator.State.PENDING_DEPLOY, o1p1.getState());
    o1 = (StatsListeningOperator)o1p1.getOperatorMeta().getOperator();
    Assert.assertEquals("containerId", originalContainer.getExternalId(), o1p1.getContainer().getExternalId());
    Assert.assertEquals("stats listener", 1, o1p1.statsListeners.size());
    Assert.assertEquals("number stats calls", 0,  o1.processStatsCnt); // stats are not logged
    Assert.assertEquals("post restore 1", PTContainer.State.ALLOCATED, o1p1.getContainer().getState());
    Assert.assertEquals("post restore 1", originalContainer.bufferServerAddress, o1p1.getContainer().bufferServerAddress);

    StreamingContainerAgent sca = scm.getContainerAgent(originalContainer.getExternalId());
    Assert.assertNotNull("allocated container restored " + originalContainer, sca);
    Assert.assertEquals("memory usage allocated container", (int)OperatorContext.MEMORY_MB.defaultValue, sca.container.getAllocatedMemoryMB());

    // YARN-1490 - simulate container terminated on AM recovery
    scm.scheduleContainerRestart(originalContainer.getExternalId());
    Assert.assertEquals("memory usage of failed container", 0, sca.container.getAllocatedMemoryMB());

    Checkpoint firstCheckpoint = new Checkpoint(3, 0, 0);
    mc = new MockContainer(scm, o1p1.getContainer());
    checkpoint(scm, o1p1, firstCheckpoint);
    mc.stats(o1p1.getId()).deployState(OperatorHeartbeat.DeployState.ACTIVE).currentWindowId(3).checkpointWindowId(3);
    mc.sendHeartbeat();
    Assert.assertEquals("state " + o1p1, PTOperator.State.ACTIVE, o1p1.getState());

    // logical plan modification triggers snapshot
    CreateOperatorRequest cor = new CreateOperatorRequest();
    cor.setOperatorFQCN(GenericTestOperator.class.getName());
    cor.setOperatorName("o2");
    CreateStreamRequest csr = new CreateStreamRequest();
    csr.setSourceOperatorName("o1");
    csr.setSourceOperatorPortName("outport");
    csr.setSinkOperatorName("o2");
    csr.setSinkOperatorPortName("inport1");
    FutureTask<?> lpmf = scm.logicalPlanModification(Lists.newArrayList(cor, csr));
    while (!lpmf.isDone()) {
      scm.monitorHeartbeat();
    }
    Assert.assertNull(lpmf.get()); // unmask exception, if any

    Assert.assertSame("dag references", dag, scm.getLogicalPlan());
    Assert.assertEquals("number operators after plan modification", 2, dag.getAllOperators().size());

    // set operator state triggers journal write
    o1p1.setState(PTOperator.State.INACTIVE);

    Checkpoint offlineCheckpoint = new Checkpoint(10, 0, 0);
    // write checkpoint while AM is out,
    // it needs to be picked up as part of restore
    checkpoint(scm, o1p1, offlineCheckpoint);

    // test restore
    dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);
    scm = StreamingContainerManager.getInstance(new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false)), dag, false);

    Assert.assertNotSame("dag references", dag, scm.getLogicalPlan());
    Assert.assertEquals("number operators after restore", 2, scm.getLogicalPlan().getAllOperators().size());

    dag = scm.getLogicalPlan();
    plan = scm.getPhysicalPlan();

    o1p1 = plan.getOperators(dag.getOperatorMeta("o1")).get(0);
    Assert.assertEquals("post restore state " + o1p1, PTOperator.State.INACTIVE, o1p1.getState());
    o1 = (StatsListeningOperator)o1p1.getOperatorMeta().getOperator();
    Assert.assertEquals("stats listener", 1, o1p1.statsListeners.size());
    Assert.assertEquals("number stats calls post restore", 1,  o1.processStatsCnt);
    Assert.assertEquals("post restore 1", PTContainer.State.ACTIVE, o1p1.getContainer().getState());
    Assert.assertEquals("post restore 1", originalContainer.bufferServerAddress, o1p1.getContainer().bufferServerAddress);

    // offline checkpoint detection
    Assert.assertEquals("checkpoints after recovery", Lists.newArrayList(firstCheckpoint, offlineCheckpoint),  o1p1.checkpoints);
  }

  @Test
  public void testWriteAheadLog() throws Exception
  {
    final MutableInt flushCount = new MutableInt();
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new FSStorageAgent(testMeta.dir, null));

    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan plan = scm.getPhysicalPlan();

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);
    Assert.assertEquals(PTOperator.State.PENDING_DEPLOY, o1p1.getState());

    ByteArrayOutputStream bos = new ByteArrayOutputStream() {
      @Override
      public void flush() throws IOException {
        super.flush();
        flushCount.increment();
      }
    };
    Journal j = new Journal();
    j.setOutputStream(new DataOutputStream(bos));
    j.register(1, new SetOperatorState(scm));
    j.register(2, new SetContainerState(scm));

    SetOperatorState op1 = new SetOperatorState(scm);
    op1.operatorId = o1p1.getId();
    op1.state = PTOperator.State.ACTIVE;
    j.write(op1);
    Assert.assertEquals("flush count", 1, flushCount.intValue());

    Assert.assertEquals(PTOperator.State.PENDING_DEPLOY, o1p1.getState());
    bos.close();

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    j.replay(new DataInputStream(bis));
    Assert.assertEquals(PTOperator.State.ACTIVE, o1p1.getState());

    InetSocketAddress addr1 = InetSocketAddress.createUnresolved("host1", 1);
    PTContainer c1 = plan.getContainers().get(0);
    c1.setState(PTContainer.State.ALLOCATED);
    c1.setExternalId("eid1");
    c1.host = "host1";
    c1.bufferServerAddress = addr1;
    c1.setAllocatedMemoryMB(2);
    c1.setRequiredMemoryMB(1);

    SetContainerState scs = new SetContainerState(scm);
    scs.container = c1;
    j.write(scs);

    c1.setExternalId(null);
    c1.setState(PTContainer.State.NEW);
    c1.setExternalId(null);
    c1.host = null;
    c1.bufferServerAddress = null;

    bis = new ByteArrayInputStream(bos.toByteArray());
    j.replay(new DataInputStream(bis));

    Assert.assertEquals("eid1", c1.getExternalId());
    Assert.assertEquals(PTContainer.State.ALLOCATED, c1.getState());
    Assert.assertEquals("host1", c1.host);
    Assert.assertEquals(addr1, c1.bufferServerAddress);
    Assert.assertEquals(1, c1.getRequiredMemoryMB());
    Assert.assertEquals(2, c1.getAllocatedMemoryMB());

  }

  @Test
  public void testRestartApp() throws Exception
  {
    FileUtils.deleteDirectory(new File(testMeta.dir)); // clean any state from previous run
    String appId1 = "app1";
    String appId2 = "app2";
    String appPath1 = testMeta.dir + "/" + appId1;
    String appPath2 = testMeta.dir + "/" + appId2;

    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_ID, appId1);
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, appPath1);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new FSStorageAgent(appPath1 + "/" + LogicalPlan.SUBDIR_CHECKPOINTS, null));
    dag.addOperator("o1", StatsListeningOperator.class);

    FSRecoveryHandler recoveryHandler = new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false));
    StreamingContainerManager.getInstance(recoveryHandler, dag, false);

    // test restore initial snapshot + log
    dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, appPath1);
    StreamingContainerManager scm = StreamingContainerManager.getInstance(new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false)), dag, false);
    PhysicalPlan plan = scm.getPhysicalPlan();
    dag = plan.getLogicalPlan(); // original plan

    Assert.assertNotNull("operator", dag.getOperatorMeta("o1"));
    PTOperator o1p1 = plan.getOperators(dag.getOperatorMeta("o1")).get(0);
    long[] ids = new FSStorageAgent(appPath1 + "/" + LogicalPlan.SUBDIR_CHECKPOINTS, new Configuration(false)).getWindowIds(o1p1.getId());
    Assert.assertArrayEquals(new long[] {o1p1.getRecoveryCheckpoint().getWindowId()}, ids);

    Assert.assertNull(o1p1.getContainer().getExternalId());
    // trigger journal write
    o1p1.getContainer().setExternalId("cid1");
    scm.writeJournal(SetContainerState.newInstance(o1p1.getContainer()));

    dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, appPath2);
    dag.setAttribute(LogicalPlan.APPLICATION_ID, appId2);
    StramClient sc = new StramClient(new Configuration(false), dag);
    try {
      sc.start();
      sc.copyInitialState(new Path(appPath1));
    }
    finally {
      sc.stop();
    }
    scm = StreamingContainerManager.getInstance(new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false)), dag, false);
    plan = scm.getPhysicalPlan();
    dag = plan.getLogicalPlan();
    Assert.assertEquals("modified appId", appId2, dag.getValue(LogicalPlan.APPLICATION_ID));
    Assert.assertEquals("modified appPath", appPath2, dag.getValue(LogicalPlan.APPLICATION_PATH));
    Assert.assertNotNull("operator", dag.getOperatorMeta("o1"));
    o1p1 = plan.getOperators(dag.getOperatorMeta("o1")).get(0);
    Assert.assertEquals("journal copied", "cid1", o1p1.getContainer().getExternalId());

    ids = new FSStorageAgent(appPath2 + "/" + LogicalPlan.SUBDIR_CHECKPOINTS, new Configuration(false)).getWindowIds(o1p1.getId());
    Assert.assertArrayEquals("checkpoints copied", new long[] {o1p1.getRecoveryCheckpoint().getWindowId()}, ids);

  }

  @Test
  public void testRpcFailover() throws Exception
  {
    String appPath = testMeta.dir;
    Configuration conf = new Configuration(false);
    final AtomicBoolean timedout = new AtomicBoolean();

    StreamingContainerUmbilicalProtocol impl = MockitoUtil.mockProtocol(StreamingContainerUmbilicalProtocol.class);

    Mockito.doAnswer(new org.mockito.stubbing.Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        LOG.debug("got call: " + invocation.getMethod());
        if (!timedout.get()) {
          try {
            timedout.set(true);
            Thread.sleep(1000);
          } catch (Exception e) {
          }
          //throw new RuntimeException("fail");
        }
        return null;
      }})
    .when(impl).log("containerId", "timeout");

    Server server = new RPC.Builder(conf).setProtocol(StreamingContainerUmbilicalProtocol.class).setInstance(impl)
        .setBindAddress("0.0.0.0").setPort(0).setNumHandlers(1).setVerbose(false).build();
    server.start();
    InetSocketAddress address = NetUtils.getConnectAddress(server);
    LOG.info("Mock server listening at " + address);

    int rpcTimeoutMillis = 500;
    int retryDelayMillis = 100;
    int retryTimeoutMillis = 500;

    FSRecoveryHandler recoveryHandler = new FSRecoveryHandler(appPath, conf);
    URI uri = RecoverableRpcProxy.toConnectURI(address, rpcTimeoutMillis, retryDelayMillis, retryTimeoutMillis);
    recoveryHandler.writeConnectUri(uri.toString());

    RecoverableRpcProxy rp = new RecoverableRpcProxy(appPath, conf);
    StreamingContainerUmbilicalProtocol protocolProxy = rp.getProxy();
    protocolProxy.log("containerId", "msg");
    // simulate socket read timeout
    try {
      protocolProxy.log("containerId", "timeout");
      Assert.fail("expected socket timeout");
    } catch (java.net.SocketTimeoutException e) {
      // expected
    }
    Assert.assertTrue("timedout", timedout.get());
    rp.close();

    // test success on retry
    timedout.set(false);
    retryTimeoutMillis = 1500;
    uri = RecoverableRpcProxy.toConnectURI(address, rpcTimeoutMillis, retryDelayMillis, retryTimeoutMillis);
    recoveryHandler.writeConnectUri(uri.toString());

    protocolProxy.log("containerId", "timeout");
    Assert.assertTrue("timedout", timedout.get());

    rp.close();
    server.stop();
  }

}

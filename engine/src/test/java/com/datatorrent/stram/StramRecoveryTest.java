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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.util.CascadeStorageAgent;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.FSStorageAgent;
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
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;

import static org.junit.Assert.assertEquals;

public class StramRecoveryTest
{
  private static final Logger LOG = LoggerFactory.getLogger(StramRecoveryTest.class);

  @Rule
  public final TestMeta testMeta = new TestMeta();

  private LogicalPlan dag;

  @Before
  public void setup()
  {
    dag = StramTestSupport.createDAG(testMeta);
  }

  private void testPhysicalPlanSerialization(StorageAgent agent) throws Exception
  {
    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    PartitioningTestOperator o2 = dag.addOperator("o2", PartitioningTestOperator.class);
    o2.setPartitionCount(3);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1, o2.inportWithCodec);
    dag.addStream("mergeStream", o2.outport1, o3.inport1);

    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);

    TestPlanContext ctx = new TestPlanContext();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, agent);
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
    assertEquals(3, o2Partitions.size());
    for (PTOperator o : o2Partitions) {
      Assert.assertNotNull("partition null " + o, o.getPartitionKeys());
      assertEquals("partition keys " + o + " " + o.getPartitionKeys(), 2, o.getPartitionKeys().size());
      PartitioningTestOperator partitionedInstance = (PartitioningTestOperator)plan.loadOperator(o);
      assertEquals("instance per partition", o.getPartitionKeys().values().toString(), partitionedInstance.pks);
      Assert.assertNotNull("partition stats null " + o, o.stats);
    }

  }

  @Test
  public void testPhysicalPlanSerializationWithSyncAgent() throws Exception
  {
    testPhysicalPlanSerialization(new FSStorageAgent(testMeta.getPath(), null));
  }

  @Test
  public void testPhysicalPlanSerializationWithAsyncAgent() throws Exception
  {
    testPhysicalPlanSerialization(new AsyncFSStorageAgent(testMeta.getPath(), null));
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
  private void testContainerManager(StorageAgent agent) throws Exception
  {
    dag.setAttribute(OperatorContext.STORAGE_AGENT, agent);

    StatsListeningOperator o1 = dag.addOperator("o1", StatsListeningOperator.class);

    FSRecoveryHandler recoveryHandler = new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false));
    StreamingContainerManager scm = StreamingContainerManager.getInstance(recoveryHandler, dag, false);
    File expFile = new File(recoveryHandler.getDir(), FSRecoveryHandler.FILE_SNAPSHOT);
    Assert.assertTrue("snapshot file " + expFile, expFile.exists());

    PhysicalPlan plan = scm.getPhysicalPlan();
    assertEquals("number required containers", 1, plan.getContainers().size());

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);

    @SuppressWarnings("UnusedAssignment") /* sneaky: the constructor does some changes to the container */
    MockContainer mc = new MockContainer(scm, o1p1.getContainer());
    PTContainer originalContainer = o1p1.getContainer();

    Assert.assertNotNull(o1p1.getContainer().bufferServerAddress);
    assertEquals(PTContainer.State.ACTIVE, o1p1.getContainer().getState());
    assertEquals("state " + o1p1, PTOperator.State.PENDING_DEPLOY, o1p1.getState());

    // test restore initial snapshot + log
    dag = StramTestSupport.createDAG(testMeta);
    scm = StreamingContainerManager.getInstance(new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false)), dag, false);
    dag = scm.getLogicalPlan();
    plan = scm.getPhysicalPlan();

    o1p1 = plan.getOperators(dag.getOperatorMeta("o1")).get(0);
    assertEquals("post restore state " + o1p1, PTOperator.State.PENDING_DEPLOY, o1p1.getState());
    o1 = (StatsListeningOperator)o1p1.getOperatorMeta().getOperator();
    assertEquals("containerId", originalContainer.getExternalId(), o1p1.getContainer().getExternalId());
    assertEquals("stats listener", 1, o1p1.statsListeners.size());
    assertEquals("number stats calls", 0, o1.processStatsCnt); // stats are not logged
    assertEquals("post restore 1", PTContainer.State.ALLOCATED, o1p1.getContainer().getState());
    assertEquals("post restore 1", originalContainer.bufferServerAddress, o1p1.getContainer().bufferServerAddress);

    StreamingContainerAgent sca = scm.getContainerAgent(originalContainer.getExternalId());
    Assert.assertNotNull("allocated container restored " + originalContainer, sca);
    assertEquals("memory usage allocated container", (int)OperatorContext.MEMORY_MB.defaultValue, sca.container.getAllocatedMemoryMB());

    // YARN-1490 - simulate container terminated on AM recovery
    scm.scheduleContainerRestart(originalContainer.getExternalId());
    assertEquals("memory usage of failed container", 0, sca.container.getAllocatedMemoryMB());

    Checkpoint firstCheckpoint = new Checkpoint(3, 0, 0);
    mc = new MockContainer(scm, o1p1.getContainer());
    checkpoint(scm, o1p1, firstCheckpoint);
    mc.stats(o1p1.getId()).deployState(OperatorHeartbeat.DeployState.ACTIVE).currentWindowId(3).checkpointWindowId(3);
    mc.sendHeartbeat();
    assertEquals("state " + o1p1, PTOperator.State.ACTIVE, o1p1.getState());

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
      scm.monitorHeartbeat(false);
    }
    Assert.assertNull(lpmf.get()); // unmask exception, if any

    Assert.assertSame("dag references", dag, scm.getLogicalPlan());
    assertEquals("number operators after plan modification", 2, dag.getAllOperators().size());

    // set operator state triggers journal write
    o1p1.setState(PTOperator.State.INACTIVE);

    Checkpoint offlineCheckpoint = new Checkpoint(10, 0, 0);
    // write checkpoint while AM is out,
    // it needs to be picked up as part of restore
    checkpoint(scm, o1p1, offlineCheckpoint);

    // test restore
    dag = StramTestSupport.createDAG(testMeta);
    scm = StreamingContainerManager.getInstance(new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false)), dag, false);

    Assert.assertNotSame("dag references", dag, scm.getLogicalPlan());
    assertEquals("number operators after restore", 2, scm.getLogicalPlan().getAllOperators().size());

    dag = scm.getLogicalPlan();
    plan = scm.getPhysicalPlan();

    o1p1 = plan.getOperators(dag.getOperatorMeta("o1")).get(0);
    assertEquals("post restore state " + o1p1, PTOperator.State.INACTIVE, o1p1.getState());
    o1 = (StatsListeningOperator)o1p1.getOperatorMeta().getOperator();
    assertEquals("stats listener", 1, o1p1.statsListeners.size());
    assertEquals("number stats calls post restore", 1, o1.processStatsCnt);
    assertEquals("post restore 1", PTContainer.State.ACTIVE, o1p1.getContainer().getState());
    assertEquals("post restore 1", originalContainer.bufferServerAddress, o1p1.getContainer().bufferServerAddress);

    // offline checkpoint detection
    assertEquals("checkpoints after recovery", Lists.newArrayList(firstCheckpoint, offlineCheckpoint), o1p1.checkpoints);
  }

  @Test
  public void testContainerManagerWithSyncAgent() throws Exception
  {
    testPhysicalPlanSerialization(new FSStorageAgent(testMeta.getPath(), null));
  }

  @Test
  public void testContainerManagerWithAsyncAgent() throws Exception
  {
    testPhysicalPlanSerialization(new AsyncFSStorageAgent(testMeta.getPath(), null));
  }

  @Test
  public void testWriteAheadLog() throws Exception
  {
    final MutableInt flushCount = new MutableInt();
    final MutableBoolean isClosed = new MutableBoolean(false);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new FSStorageAgent(testMeta.getPath(), null));

    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan plan = scm.getPhysicalPlan();
    Journal j = scm.getJournal();
    ByteArrayOutputStream bos = new ByteArrayOutputStream()
    {
      @Override
      public void flush() throws IOException
      {
        super.flush();
        flushCount.increment();
      }

      @Override
      public void close() throws IOException
      {
        super.close();
        isClosed.setValue(true);
      }
    };
    j.setOutputStream(new DataOutputStream(bos));

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);
    assertEquals(PTOperator.State.PENDING_DEPLOY, o1p1.getState());
    String externalId = new MockContainer(scm, o1p1.getContainer()).container.getExternalId();
    assertEquals("flush count", 1, flushCount.intValue());

    o1p1.setState(PTOperator.State.ACTIVE);
    assertEquals(PTOperator.State.ACTIVE, o1p1.getState());
    assertEquals("flush count", 2, flushCount.intValue());
    assertEquals("is closed", false, isClosed.booleanValue());

    // this will close the stream. There are 2 calls to flush() during the close() - one in Kryo Output and one
    // in FilterOutputStream
    j.setOutputStream(null);
    assertEquals("flush count", 4, flushCount.intValue());
    assertEquals("is closed", true, isClosed.booleanValue());

    // output stream is closed, so state will be changed without recording it in the journal
    o1p1.setState(PTOperator.State.INACTIVE);
    assertEquals(PTOperator.State.INACTIVE, o1p1.getState());
    assertEquals("flush count", 4, flushCount.intValue());

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    j.replay(new DataInputStream(bis));
    assertEquals(PTOperator.State.ACTIVE, o1p1.getState());

    InetSocketAddress addr1 = InetSocketAddress.createUnresolved("host1", 1);
    PTContainer c1 = plan.getContainers().get(0);
    c1.setState(PTContainer.State.ALLOCATED);
    c1.host = "host1";
    c1.bufferServerAddress = addr1;
    c1.setAllocatedMemoryMB(2);
    c1.setRequiredMemoryMB(1);
    c1.setAllocatedVCores(3);
    c1.setRequiredVCores(4);

    j.setOutputStream(new DataOutputStream(bos));
    j.write(c1.getSetContainerState());

    c1.setExternalId(null);
    c1.setState(PTContainer.State.NEW);
    c1.setExternalId(null);
    c1.host = null;
    c1.bufferServerAddress = null;

    bis = new ByteArrayInputStream(bos.toByteArray());
    j.replay(new DataInputStream(bis));

    assertEquals(externalId, c1.getExternalId());
    assertEquals(PTContainer.State.ALLOCATED, c1.getState());
    assertEquals("host1", c1.host);
    assertEquals(addr1, c1.bufferServerAddress);
    assertEquals(1, c1.getRequiredMemoryMB());
    assertEquals(2, c1.getAllocatedMemoryMB());
    assertEquals(3, c1.getAllocatedVCores());
    assertEquals(4, c1.getRequiredVCores());

    j.write(scm.getSetOperatorProperty("o1", "maxTuples", "100"));
    o1.setMaxTuples(10);
    j.setOutputStream(null);
    bis = new ByteArrayInputStream(bos.toByteArray());
    j.replay(new DataInputStream(bis));
    assertEquals(100, o1.getMaxTuples());

    j.setOutputStream(new DataOutputStream(bos));
    scm.setOperatorProperty("o1", "maxTuples", "10");
    assertEquals(10, o1.getMaxTuples());
    o1.setMaxTuples(100);
    assertEquals(100, o1.getMaxTuples());
    j.setOutputStream(null);

    bis = new ByteArrayInputStream(bos.toByteArray());
    j.replay(new DataInputStream(bis));
    assertEquals(10, o1.getMaxTuples());

    j.setOutputStream(new DataOutputStream(bos));
    scm.setPhysicalOperatorProperty(o1p1.getId(), "maxTuples", "50");
  }

  private void testRestartApp(StorageAgent agent, String appPath1) throws Exception
  {
    String appId1 = "app1";
    String appId2 = "app2";
    String appPath2 = testMeta.getPath() + "/" + appId2;

    dag.setAttribute(LogicalPlan.APPLICATION_ID, appId1);
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, appPath1);
    dag.setAttribute(LogicalPlan.APPLICATION_ATTEMPT_ID, 1);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, agent);
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
    long[] ids = new FSStorageAgent(appPath1 + "/" + LogicalPlan.SUBDIR_CHECKPOINTS, new Configuration()).getWindowIds(o1p1.getId());
    Assert.assertArrayEquals(new long[] {o1p1.getRecoveryCheckpoint().getWindowId()}, ids);
    Assert.assertNull(o1p1.getContainer().getExternalId());
    // trigger journal write
    o1p1.getContainer().setExternalId("cid1");
    scm.writeJournal(o1p1.getContainer().getSetContainerState());

    /* simulate application restart from app1 */
    dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, appPath2);
    dag.setAttribute(LogicalPlan.APPLICATION_ID, appId2);
    StramClient sc = new StramClient(new Configuration(), dag);
    try {
      sc.start();
      sc.copyInitialState(new Path(appPath1));
    } finally {
      sc.stop();
    }
    scm = StreamingContainerManager.getInstance(new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false)), dag, false);
    plan = scm.getPhysicalPlan();
    dag = plan.getLogicalPlan();
    assertEquals("modified appId", appId2, dag.getValue(LogicalPlan.APPLICATION_ID));
    assertEquals("modified appPath", appPath2, dag.getValue(LogicalPlan.APPLICATION_PATH));
    Assert.assertNotNull("operator", dag.getOperatorMeta("o1"));
    o1p1 = plan.getOperators(dag.getOperatorMeta("o1")).get(0);
    assertEquals("journal copied", "cid1", o1p1.getContainer().getExternalId());

    CascadeStorageAgent csa = (CascadeStorageAgent)dag.getAttributes().get(OperatorContext.STORAGE_AGENT);
    Assert.assertEquals("storage agent is replaced by cascade", csa.getClass(), CascadeStorageAgent.class);
    Assert.assertEquals("current storage agent is of same type", csa.getCurrentStorageAgent().getClass(), agent.getClass());
    Assert.assertEquals("parent storage agent is of same type ", csa.getParentStorageAgent().getClass(), agent.getClass());
    /* parent and current points to expected location */
    Assert.assertEquals(true, ((FSStorageAgent)csa.getParentStorageAgent()).path.contains("app1"));
    Assert.assertEquals(true, ((FSStorageAgent)csa.getCurrentStorageAgent()).path.contains("app2"));

    ids = csa.getWindowIds(o1p1.getId());
    Assert.assertArrayEquals("checkpoints copied", new long[] {o1p1.getRecoveryCheckpoint().getWindowId()}, ids);


    /* simulate another application restart from app2 */
    String appId3 = "app3";
    String appPath3 = testMeta.getPath() + "/" + appId3;
    dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, appPath3);
    dag.setAttribute(LogicalPlan.APPLICATION_ID, appId3);
    sc = new StramClient(new Configuration(), dag);
    try {
      sc.start();
      sc.copyInitialState(new Path(appPath2)); // copy state from app2.
    } finally {
      sc.stop();
    }
    scm = StreamingContainerManager.getInstance(new FSRecoveryHandler(dag.assertAppPath(), new Configuration(false)), dag, false);
    plan = scm.getPhysicalPlan();
    dag = plan.getLogicalPlan();

    csa = (CascadeStorageAgent)dag.getAttributes().get(OperatorContext.STORAGE_AGENT);
    Assert.assertEquals("storage agent is replaced by cascade", csa.getClass(), CascadeStorageAgent.class);
    Assert.assertEquals("current storage agent is of same type", csa.getCurrentStorageAgent().getClass(), agent.getClass());
    Assert.assertEquals("parent storage agent is of same type ", csa.getParentStorageAgent().getClass(), CascadeStorageAgent.class);

    CascadeStorageAgent parent = (CascadeStorageAgent)csa.getParentStorageAgent();
    Assert.assertEquals("current storage agent is of same type ", parent.getCurrentStorageAgent().getClass(), agent.getClass());
    Assert.assertEquals("parent storage agent is cascade ", parent.getParentStorageAgent().getClass(), agent.getClass());
    /* verify paths */
    Assert.assertEquals(true, ((FSStorageAgent)parent.getParentStorageAgent()).path.contains("app1"));
    Assert.assertEquals(true, ((FSStorageAgent)parent.getCurrentStorageAgent()).path.contains("app2"));
    Assert.assertEquals(true, ((FSStorageAgent)csa.getCurrentStorageAgent()).path.contains("app3"));

    ids = csa.getWindowIds(o1p1.getId());
    Assert.assertArrayEquals("checkpoints copied", new long[] {o1p1.getRecoveryCheckpoint().getWindowId()}, ids);
  }

  @Test
  public void testRestartAppWithSyncAgent() throws Exception
  {
    final String appPath1 = testMeta.getPath() + "/app1";
    testRestartApp(new FSStorageAgent(appPath1 + "/" + LogicalPlan.SUBDIR_CHECKPOINTS, null), appPath1);
  }

  @Test
  public void testRestartAppWithAsyncAgent() throws Exception
  {
    final String appPath1 = testMeta.getPath() + "/app1";
    testRestartApp(new AsyncFSStorageAgent(appPath1 + "/" + LogicalPlan.SUBDIR_CHECKPOINTS, null), appPath1);
  }

  @Test
  public void testRpcFailover() throws Exception
  {
    String appPath = testMeta.getPath();
    Configuration conf = new Configuration(false);
    final AtomicBoolean timedout = new AtomicBoolean();

    StreamingContainerUmbilicalProtocol impl = Mockito.mock(StreamingContainerUmbilicalProtocol.class, Mockito.withSettings().extraInterfaces(Closeable.class));

    final Answer<Void> answer = new Answer<Void>()
    {
      @Override
      public Void answer(InvocationOnMock invocation)
      {
        LOG.debug("got call: " + invocation.getMethod());
        if (!timedout.get()) {
          try {
            timedout.set(true);
            Thread.sleep(1000);
          } catch (Exception e) {
            // ignore
          }
          //throw new RuntimeException("fail");
        }
        return null;
      }
    };
    Mockito.doAnswer(answer).when(impl).log("containerId", "timeout");
    Mockito.doAnswer(answer).when(impl).reportError("containerId", null, "timeout", null);

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

    String rpcTimeout = System.getProperty(RecoverableRpcProxy.RPC_TIMEOUT);
    String rpcRetryDelay = System.getProperty(RecoverableRpcProxy.RETRY_DELAY);
    String rpcRetryTimeout = System.getProperty(RecoverableRpcProxy.RETRY_TIMEOUT);

    System.setProperty(RecoverableRpcProxy.RPC_TIMEOUT, Integer.toString(500));
    System.setProperty(RecoverableRpcProxy.RETRY_DELAY, Long.toString(100));
    System.setProperty(RecoverableRpcProxy.RETRY_TIMEOUT, Long.toString(500));

    timedout.set(false);
    uri = RecoverableRpcProxy.toConnectURI(address);
    recoveryHandler.writeConnectUri(uri.toString());

    rp = new RecoverableRpcProxy(appPath, conf);
    protocolProxy = rp.getProxy();
    protocolProxy.reportError("containerId", null, "msg", null);
    try {
      protocolProxy.log("containerId", "timeout");
      Assert.fail("expected socket timeout");
    } catch (java.net.SocketTimeoutException e) {
      // expected
    }
    Assert.assertTrue("timedout", timedout.get());
    rp.close();

    timedout.set(false);
    System.setProperty(RecoverableRpcProxy.RETRY_TIMEOUT, Long.toString(1500));

    uri = RecoverableRpcProxy.toConnectURI(address);
    recoveryHandler.writeConnectUri(uri.toString());

    protocolProxy.reportError("containerId", null, "timeout", null);
    Assert.assertTrue("timedout", timedout.get());

    restoreSystemProperty(RecoverableRpcProxy.RPC_TIMEOUT, rpcTimeout);
    restoreSystemProperty(RecoverableRpcProxy.RETRY_DELAY, rpcRetryDelay);
    restoreSystemProperty(RecoverableRpcProxy.RETRY_TIMEOUT, rpcRetryTimeout);

    server.stop();
  }

  private static String restoreSystemProperty(final String key, final String value)
  {
    return (value == null) ? System.clearProperty(key) : System.setProperty(key, value);
  }

}

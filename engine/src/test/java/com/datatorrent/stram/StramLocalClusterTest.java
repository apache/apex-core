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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster.LocalStreamingContainer;
import com.datatorrent.stram.StramLocalCluster.MockComponentFactory;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.engine.TestOutputOperator;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.support.ManualScheduledExecutorService;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.util.VersionInfo;

public class StramLocalClusterTest
{
  private static final Logger LOG = LoggerFactory.getLogger(StramLocalClusterTest.class);

  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  private LogicalPlan dag;

  @Before
  public void setup() throws IOException
  {
    dag = StramTestSupport.createDAG(testMeta);
  }

  @After
  public void teardown()
  {
//    StramChild.eventloop.stop();
  }

  /**
   * Verify test configuration launches and stops after input terminates.
   * Test validates expected output end to end.
   *
   * @throws Exception
   */
  @Test
  public void testLocalClusterInitShutdown() throws Exception
  {
    TestGeneratorInputOperator genNode = dag.addOperator("genNode", TestGeneratorInputOperator.class);
    genNode.setMaxTuples(2);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    node1.setEmitFormat("%s >> node1");

    File outFile = new File("./target/" + StramLocalClusterTest.class.getName() + "-testLocalClusterInitShutdown.out");
    outFile.delete();

    TestOutputOperator outNode = dag.addOperator("outNode", TestOutputOperator.class);
    outNode.pathSpec = outFile.toURI().toString();

    dag.addStream("fromGenNode", genNode.outport, node1.inport1);

    dag.addStream("fromNode1", node1.outport1, outNode.inport);

    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 2);

    StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.setHeartbeatMonitoringEnabled(false);
    localCluster.run();

    Assert.assertTrue(outFile + " exists", outFile.exists());
    LineNumberReader lnr = new LineNumberReader(new FileReader(outFile));
    String line;
    while ((line = lnr.readLine()) != null) {
      Assert.assertTrue("line match " + line, line.matches("" + lnr.getLineNumber() + " >> node1"));
    }
    Assert.assertEquals("number lines", 2, lnr.getLineNumber());
    lnr.close();
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testRecovery() throws Exception
  {
    AsyncFSStorageAgent agent = new AsyncFSStorageAgent(testMeta.getPath(), null);
    agent.setSyncCheckpoint(true);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, agent);

    TestGeneratorInputOperator node1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    // data will be added externally from test
    node1.setMaxTuples(0);

    GenericTestOperator node2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.addStream("o1o2", node1.outport, node2.inport1);

    dag.validate();

    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);

    final ManualScheduledExecutorService wclock = new ManualScheduledExecutorService(1);

    MockComponentFactory mcf = new MockComponentFactory()
    {
      @Override
      public WindowGenerator setupWindowGenerator()
      {
        WindowGenerator wingen = StramTestSupport.setupWindowGenerator(wclock);
        wingen.setCheckpointCount(2, 0);
        return wingen;
      }

    };

    StramLocalCluster localCluster = new StramLocalCluster(dag, mcf);
    localCluster.setPerContainerBufferServer(true);
    localCluster.setHeartbeatMonitoringEnabled(false); // driven by test
    localCluster.runAsync();

    PTOperator ptNode1 = localCluster.findByLogicalNode(dag.getMeta(node1));
    PTOperator ptNode2 = localCluster.findByLogicalNode(dag.getMeta(node2));

    LocalStreamingContainer c0 = StramTestSupport.waitForActivation(localCluster, ptNode1);
    Map<Integer, Node<?>> nodeMap = c0.getNodes();
    Assert.assertEquals("number operators", 1, nodeMap.size());
    TestGeneratorInputOperator n1 = (TestGeneratorInputOperator)nodeMap.get(ptNode1.getId()).getOperator();
    Assert.assertNotNull(n1);

    LocalStreamingContainer c2 = StramTestSupport.waitForActivation(localCluster, ptNode2);
    Map<Integer, Node<?>> c2NodeMap = c2.getNodes();
    Assert.assertEquals("number operators downstream", 1, c2NodeMap.size());
    GenericTestOperator n2 = (GenericTestOperator)c2NodeMap.get(localCluster.findByLogicalNode(dag.getMeta(node2)).getId()).getOperator();
    Assert.assertNotNull(n2);

    // input data
    String tuple1 = "tuple1";
    n1.addTuple(tuple1);

    OperatorContext n1Context = c0.getNodeContext(ptNode1.getId());
    Assert.assertEquals("initial window id", -1, n1Context.getLastProcessedWindowId());
    wclock.tick(1); // checkpoint window
    wclock.tick(1);
    Assert.assertEquals("current window", 2, wclock.getCurrentTimeMillis());

    OperatorContext o2Context = c2.getNodeContext(ptNode2.getId());
    Assert.assertNotNull("context ", o2Context);

    StramTestSupport.waitForWindowComplete(o2Context, 1);
    Assert.assertEquals("o2 received ", tuple1, n2.inport1Tuple);

    wclock.tick(1);
    Assert.assertEquals("current window", 3, wclock.getCurrentTimeMillis());
    // checkpoint between window 1 and 2
    StramTestSupport.waitForWindowComplete(o2Context, 2);

    // propagate checkpoints to master
    c0.triggerHeartbeat();
    // wait for heartbeat cycle to complete
    c0.waitForHeartbeat(5000);
    Assert.assertEquals("checkpoint " + ptNode1, 1, ptNode1.getRecentCheckpoint().windowId);
    c2.triggerHeartbeat();
    //Thread.yield();
    Thread.sleep(1); // yield without using yield for heartbeat cycle
    c2.waitForHeartbeat(5000);
    Assert.assertEquals("checkpoint " + ptNode2, 1, ptNode2.getRecentCheckpoint().windowId);

    Assert.assertEquals("checkpoints " + ptNode1, Arrays.asList(new Checkpoint[] {new Checkpoint(1L, 0, 0)}), ptNode1.checkpoints);
    Assert.assertEquals("checkpoints " + ptNode2, Arrays.asList(new Checkpoint[] {new Checkpoint(1L, 0, 0)}), ptNode2.checkpoints);

    //
    // simulate container failure (operator o1)
    //
    localCluster.failContainer(c0);

    // replacement container starts empty
    // operators will deploy after downstream operator was removed
    LocalStreamingContainer c0Replaced = StramTestSupport.waitForActivation(localCluster, ptNode1);
    c0Replaced.triggerHeartbeat();
    c0Replaced.waitForHeartbeat(5000); // next heartbeat after setup

    Assert.assertNotSame("old container", c0, c0Replaced);
    Assert.assertNotSame("old container", c0.getContainerId(), c0Replaced.getContainerId());

    // verify change in downstream container
    LOG.debug("triggering c2 heartbeat processing");
    StreamingContainerAgent c2Agent = localCluster.getContainerAgent(c2);

    // wait for downstream re-deploy to complete
    long startTms = System.currentTimeMillis();
    while (c2Agent.hasPendingWork() && StramTestSupport.DEFAULT_TIMEOUT_MILLIS > System.currentTimeMillis() - startTms) {
      Thread.sleep(200);
      c2.triggerHeartbeat();
      LOG.debug("Waiting for {} to complete pending work.", c2.getContainerId());
    }

    Assert.assertEquals(c2.getContainerId() + " operators after redeploy " + c2.getNodes(), 1, c2.getNodes().size());
    // verify downstream operator re-deployed in existing container
    Assert.assertEquals("active " + ptNode2, c2, StramTestSupport.waitForActivation(localCluster, ptNode2));
    GenericTestOperator o2Recovered = (GenericTestOperator)c2NodeMap.get(localCluster.findByLogicalNode(dag.getMeta(node2)).getId()).getOperator();
    Assert.assertNotNull("redeployed " + ptNode2, o2Recovered);
    Assert.assertNotSame("new instance " + ptNode2, n2, o2Recovered);
    Assert.assertEquals("restored state " + ptNode2, tuple1, o2Recovered.inport1Tuple);

    TestGeneratorInputOperator o1Recovered = (TestGeneratorInputOperator)c0Replaced.getNodes().get(ptNode1.getId()).getOperator();
    Assert.assertNotNull(o1Recovered);

    OperatorContext o1RecoveredContext = c0Replaced.getNodeContext(ptNode1.getId());
    Assert.assertNotNull("active " + ptNode1, o1RecoveredContext);
    wclock.tick(1);
    Assert.assertEquals("current window", 4, wclock.getCurrentTimeMillis());

    // refresh context after operator re-deploy
    o2Context = c2.getNodeContext(ptNode2.getId());
    Assert.assertNotNull("active " + ptNode2, o2Context);

    StramTestSupport.waitForWindowComplete(o1RecoveredContext, 3);
    StramTestSupport.waitForWindowComplete(o2Context, 3);

    wclock.tick(1); // checkpoint window
    Assert.assertEquals("current window", 5, wclock.getCurrentTimeMillis());

    String tuple2 = "tuple2";
    o1Recovered.addTuple(tuple2);

    StramTestSupport.waitForWindowComplete(o1RecoveredContext, 4);
    StramTestSupport.waitForWindowComplete(o2Context, 4);
    // check data flow after recovery
    Assert.assertEquals("retrieved tuple (after recovery) " + ptNode2, tuple2, o2Recovered.inport1Tuple);

    // propagate checkpoints to master
    c0Replaced.triggerHeartbeat();
    c0Replaced.waitForHeartbeat(5000);
    c2.triggerHeartbeat();
    c2.waitForHeartbeat(5000);

    // purge checkpoints
    localCluster.dnmgr.monitorHeartbeat(false); // checkpoint purging

    Assert.assertEquals("checkpoints " + ptNode1, Arrays.asList(new Checkpoint[] {new Checkpoint(3L, 0, 0)}), ptNode1.checkpoints);
    Assert.assertEquals("checkpoints " + ptNode2, Arrays.asList(new Checkpoint[] {new Checkpoint(3L, 0, 0)}), ptNode2.checkpoints);

    localCluster.shutdown();
  }

  @Test
  public void testDynamicLoading() throws Exception
  {
    String generatedJar = generatejar("POJO");
    URLClassLoader uCl = URLClassLoader.newInstance(new URL[] {new File(generatedJar).toURI().toURL()});
    Class<?> pojo = uCl.loadClass("POJO");

    DynamicLoaderApp app = new DynamicLoaderApp();
    app.generatedJar = generatedJar;
    app.pojo = pojo;

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(app, new Configuration());
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    DynamicLoaderApp.latch.await();
    Assert.assertTrue(DynamicLoaderApp.passed);
    lc.shutdown();
  }

  static class DynamicLoaderApp implements StreamingApplication
  {
    static boolean passed = false;
    static CountDownLatch latch = new CountDownLatch(2);

    DynamicLoader test;
    String generatedJar;
    Class<?> pojo;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      TestGeneratorInputOperator input = dag.addOperator("Input", new TestGeneratorInputOperator());
      test = dag.addOperator("Test", new DynamicLoader());

      dag.addStream("S1", input.outport, test.input);
      dag.setAttribute(Context.DAGContext.LIBRARY_JARS, generatedJar);
      dag.setInputPortAttribute(test.input, Context.PortContext.TUPLE_CLASS, pojo);
    }
  }

  static class DynamicLoader extends BaseOperator
  {
    public final transient DefaultInputPort input = new DefaultInputPort()
    {
      @Override
      public void setup(Context.PortContext context)
      {
        Class<?> value = context.getValue(Context.PortContext.TUPLE_CLASS);
        if (value.getName().equals("POJO")) {
          DynamicLoaderApp.passed = true;
        } else {
          DynamicLoaderApp.passed = false;
        }
        DynamicLoaderApp.latch.countDown();
      }

      @Override
      public void process(Object tuple)
      {
      }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
        cl.loadClass("POJO");
      } catch (ClassNotFoundException e) {
        DynamicLoaderApp.passed = false;
        DynamicLoaderApp.latch.countDown();
        throw new RuntimeException(e);
      }

      try {
        Class.forName("POJO", true, Thread.currentThread().getContextClassLoader());
      } catch (ClassNotFoundException e) {
        DynamicLoaderApp.passed = false;
        DynamicLoaderApp.latch.countDown();
        throw new RuntimeException(e);
      }

      DynamicLoaderApp.passed = true;
      DynamicLoaderApp.latch.countDown();
    }
  }

  private String generatejar(String pojoClassName) throws IOException, InterruptedException
  {
    String sourceDir = "src/test/resources/dynamicJar/";
    String destDir = testMeta.getPath();

    // The compiled java class should be loadable by the current java runtime hence setting the compile target version
    // to be the same
    String binLocation = getJavaBinLocation();

    Process p = Runtime.getRuntime()
        .exec(new String[] {binLocation + "javac", "-d", destDir, sourceDir + pojoClassName + ".java"}, null, null);
    IOUtils.copy(p.getInputStream(), System.out);
    IOUtils.copy(p.getErrorStream(), System.err);
    Assert.assertEquals(0, p.waitFor());

    p = Runtime.getRuntime()
        .exec(new String[] {binLocation + "jar", "-cf", pojoClassName + ".jar", pojoClassName + ".class"}, null, new File(destDir));
    IOUtils.copy(p.getInputStream(), System.out);
    IOUtils.copy(p.getErrorStream(), System.err);
    Assert.assertEquals(0, p.waitFor());

    return new File(destDir, pojoClassName + ".jar").getAbsolutePath();
  }

  private String getJavaBinLocation()
  {
    String javaHome = System.getProperty("java.home");
    return VersionInfo.compare(System.getProperty("java.version"), "1.9") < 0 ? javaHome + "/../bin/" : javaHome + "/bin/";
  }

  @Test
  public void testAppPath() throws Exception
  {
    // add operator for initial checkpoint
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    o1.setMaxTuples(1);
    File relPath = new File(dag.getAttributes().get(DAGContext.APPLICATION_PATH));
    String uriPath = relPath.toURI().toString();
    dag.setAttribute(DAGContext.APPLICATION_PATH, uriPath);
    StramLocalCluster cluster = new StramLocalCluster(dag);
    // no need for run(), just need the initial checkpoint
    Assert.assertFalse(cluster.isFinished());
    Assert.assertTrue("app path exists", relPath.exists() && relPath.isDirectory());
    File checkPointDir = new File(relPath, LogicalPlan.SUBDIR_CHECKPOINTS);
    Assert.assertTrue("checkpoint path exists", checkPointDir.exists() && checkPointDir.isDirectory());
  }

}

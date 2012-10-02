/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.dag.DAG;
import com.malhartech.dag.GenericTestModule;
import com.malhartech.dag.Module;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.TestGeneratorInputModule;
import com.malhartech.dag.TestOutputModule;
import com.malhartech.dag.DAG.Operator;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StramLocalCluster.MockComponentFactory;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stream.StramTestSupport;

public class StramLocalClusterTest
{
  private static final Logger LOG = LoggerFactory.getLogger(StramLocalClusterTest.class);

  @Ignore
  @Test
  public void testTplg() throws IOException, Exception {
    String tplgFile = "src/test/resources/clusterTest.tplg.properties";
    StramLocalCluster lc = new StramLocalCluster(DAGPropertiesBuilder.create(new Configuration(false), tplgFile));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

  /**
   * Verify test configuration launches and stops after input terminates.
   * Test validates expected output end to end.
   * @throws Exception
   */
  @Test
  public void testLocalClusterInitShutdown() throws Exception
  {
    DAG dag = new DAG();

    Operator genNode = dag.addOperator("genNode", TestGeneratorInputModule.class);
    genNode.setProperty("maxTuples", "1");

    Operator node1 = dag.addOperator("node1", GenericTestModule.class);
    node1.setProperty("emitFormat", "%s >> node1");

    File outFile = new File("./target/" + StramLocalClusterTest.class.getName() + "-testLocalClusterInitShutdown.out");
    outFile.delete();

    Operator outNode = dag.addOperator("outNode", TestOutputModule.class);
    outNode.setProperty(TestOutputModule.P_FILEPATH, outFile.toURI().toString());

    dag.addStream("fromGenNode")
      .setSource(genNode.getOutput(TestGeneratorInputModule.OUTPUT_PORT))
      .addSink(node1.getInput(GenericTestModule.INPUT1));

    dag.addStream("fromNode1")
      .setSource(node1.getOutput(GenericTestModule.OUTPUT1))
      .addSink(outNode.getInput(TestOutputModule.PORT_INPUT));

    dag.setMaxContainerCount(2);

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
  public void testChildRecovery() throws Exception
  {
    DAG dag = new DAG();

    Operator node1 = dag.addOperator("node1", TestGeneratorInputModule.class);
    node1.setProperty(TestGeneratorInputModule.KEY_MAX_TUPLES, "10"); // TODO: need solution for graceful container shutdown
    Operator node2 = dag.addOperator("node2", GenericTestModule.class);

    dag.addStream("n1n2").
      setSource(node1.getOutput(TestGeneratorInputModule.OUTPUT_PORT)).
      addSink(node2.getInput(GenericTestModule.INPUT1));

    dag.validate();

    dag.getConf().setInt(DAG.STRAM_CHECKPOINT_INTERVAL_MILLIS, 0); // disable auto backup

    final ManualScheduledExecutorService wclock = new ManualScheduledExecutorService(1);

    MockComponentFactory mcf = new MockComponentFactory() {
      @Override
      public WindowGenerator setupWindowGenerator() {
        WindowGenerator wingen = StramTestSupport.setupWindowGenerator(wclock);
        return wingen;
      }
    };

    StramLocalCluster localCluster = new StramLocalCluster(dag, mcf);
    localCluster.runAsync();

    LocalStramChild c0 = waitForActivation(localCluster, node1);
    Map<String, Module> nodeMap = c0.getNodes();
    Assert.assertEquals("number operators", 1, nodeMap.size());

    PTOperator ptNode1 = localCluster.findByLogicalNode(node1);
    Module n1 = nodeMap.get(ptNode1.id);
    Assert.assertNotNull(n1);

    LocalStramChild c2 = waitForActivation(localCluster, node2);
    Map<String, Module> c2NodeMap = c2.getNodes();
    Assert.assertEquals("number operators downstream", 1, c2NodeMap.size());
    GenericTestModule n2 = (GenericTestModule)c2NodeMap.get(localCluster.findByLogicalNode(node2).id);
    Assert.assertNotNull(n2);

    ModuleContext n1Context = c0.getNodeContext(ptNode1.id);
    Assert.assertEquals("initial window id", 0, n1Context.getLastProcessedWindowId());
    wclock.tick(1); // begin window 1
    wclock.tick(2); // begin window 2
    StramTestSupport.waitForWindowComplete(n1Context, 1);

    backupNode(c0, n1Context); // backup window 2

    wclock.tick(1); // end window 2
    StramTestSupport.waitForWindowComplete(n1Context, 2);

    ModuleContext n2Context = c2.getNodeContext(localCluster.findByLogicalNode(node2).id);

    wclock.tick(1); // end window 3

    StramTestSupport.waitForWindowComplete(n2Context, 3);
    n2.setMyStringProperty("checkpoint3");
    backupNode(c2, n2Context); // backup window 4

    // move window forward, wait until propagated to module,
    // to ensure backup at previous window end was processed
    wclock.tick(1);
    StramTestSupport.waitForWindowComplete(n2Context, 4);

    // propagate checkpoints to master
    c0.triggerHeartbeat();
    // wait for heartbeat cycle to complete
    c0.waitForHeartbeat(5000);
    Assert.assertEquals("checkpoint propagated " + ptNode1, 2, ptNode1.getRecentCheckpoint());
    c2.triggerHeartbeat();
    Thread.yield();
    Thread.sleep(50); // the heartbeat trigger cycle does not seem to work here
    c2.waitForHeartbeat(5000);
    PTOperator ptNode2 = localCluster.findByLogicalNode(node2);
    Assert.assertEquals("checkpoint propagated " + ptNode2, 4, ptNode2.getRecentCheckpoint());

    // simulate node failure
    localCluster.failContainer(c0);

    // replacement container starts empty
    // operators will deploy after downstream node was removed
    LocalStramChild c0Replaced = waitForActivation(localCluster, node1);
    c0Replaced.triggerHeartbeat();
    c0Replaced.waitForHeartbeat(5000); // next heartbeat after setup

    Assert.assertNotSame("old container", c0, c0Replaced);
    Assert.assertNotSame("old container", c0.getContainerId(), c0Replaced.getContainerId());

    // verify change in downstream container
    LOG.debug("triggering c2 heartbeat processing");
    StramChildAgent c2Agent = localCluster.getContainerAgent(c2);

    // wait for downstream re-deploy to complete
    while (c2Agent.hasPendingWork()) {
      Thread.sleep(500);
      c2.triggerHeartbeat();
      LOG.debug("Waiting for {} to complete pending work.", c2.getContainerId());
    }

    Assert.assertEquals("downstream operators after redeploy " + c2.getNodes(), 1, c2.getNodes().size());
    // verify that the downstream node was replaced
    GenericTestModule n2Replaced = (GenericTestModule)c2NodeMap.get(localCluster.findByLogicalNode(node2).id);
    Assert.assertNotNull(n2Replaced);
    Assert.assertNotSame("node2 redeployed", n2, n2Replaced);
    Assert.assertEquals("restored state " + ptNode2, n2.getMyStringProperty(), n2Replaced.getMyStringProperty());


    Module n1Replaced = nodeMap.get(ptNode1.id);
    Assert.assertNotNull(n1Replaced);

    ModuleContext n1ReplacedContext = c0Replaced.getNodeContext(ptNode1.id);
    Assert.assertNotNull("node active " + ptNode1, n1ReplacedContext);
    // the node context should reflect the last processed window (the backup window)?
    //Assert.assertEquals("initial window id", 1, n1ReplacedContext.getLastProcessedWindowId());

    localCluster.shutdown();
  }

  /**
   * Wait until instance of operator comes online in a container and return the container reference.
   *
   * @param localCluster
   * @param nodeConf
   * @return
   * @throws InterruptedException
   */
  @SuppressWarnings("SleepWhileInLoop")
  private LocalStramChild waitForActivation(StramLocalCluster localCluster, Operator nodeDecl) throws InterruptedException
  {
    PTOperator node = localCluster.findByLogicalNode(nodeDecl);
    Assert.assertNotNull("no node for " + nodeDecl, node);

    LocalStramChild container;
    while (true) {
      if (node.container.containerId != null) {
        if ((container = localCluster.getContainer(node.container.containerId)) != null) {
          if (container.getNodeContext(node.id) != null) {
            return container;
          }
        }
      }
      try {
        LOG.debug("Waiting for {} in container {}", node, node.container.containerId);
        Thread.sleep(500);
      }
      catch (InterruptedException e) {
      }
    }
  }

  private void backupNode(StramChild c, ModuleContext nodeCtx)
  {
    StramToNodeRequest backupRequest = new StramToNodeRequest();
    backupRequest.setNodeId(nodeCtx.getId());
    backupRequest.setRequestType(RequestType.CHECKPOINT);
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.nodeRequests = Collections.singletonList(backupRequest);
    LOG.debug("Requesting backup {} node {}", c.getContainerId(), nodeCtx.getId());
    c.processHeartbeatResponse(rsp);
  }

}

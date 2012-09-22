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
import com.malhartech.stram.DAGDeployer.PTNode;
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
    StramLocalCluster lc = new StramLocalCluster(DAGPropertiesBuilder.create(new Configuration(), tplgFile));
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

  @Ignore // windows lost problem?
  @Test
  public void testChildRecovery() throws Exception
  {
    DAG dag = new DAG();

    Operator node1 = dag.addOperator("node1", TestGeneratorInputModule.class);
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

    LocalStramChild c0 = waitForContainer(localCluster, node1);
    //Thread.sleep(1000);
    Map<String, Module> nodeMap = c0.getNodes();
    Assert.assertEquals("number nodes", 1, nodeMap.size());

    PTNode ptNode1 = localCluster.findByLogicalNode(node1);
    Module n1 = nodeMap.get(ptNode1.id);
    Assert.assertNotNull(n1);

    LocalStramChild c2 = waitForContainer(localCluster, node2);
    Map<String, Module> c2NodeMap = c2.getNodes();
    Assert.assertEquals("number nodes downstream", 1, c2NodeMap.size());
    Module n2 = c2NodeMap.get(localCluster.findByLogicalNode(node2).id);
    Assert.assertNotNull(n2);

    ModuleContext n1Context = c0.getNodeContext(ptNode1.id);
    Assert.assertEquals("initial window id", 0, n1Context.getLastProcessedWindowId());
    wclock.tick(1);

    waitForWindow(n1Context, 1);
    backupNode(c0, n1Context);

    wclock.tick(1);

    ModuleContext n2Context = c2.getNodeContext(localCluster.findByLogicalNode(node2).id);
    waitForWindow(n2Context, 2);
    backupNode(c2, n2Context);

    wclock.tick(1);

    // move window forward and wait for nodes to reach,
    // to ensure backup in previous windows was processed
    wclock.tick(1);

    //waitForWindow(n1, 3);
    waitForWindow(n2Context, 3);

    // propagate checkpoints to master
    c0.triggerHeartbeat();
    // wait for heartbeat cycle to complete
    c0.waitForHeartbeat(5000);

    c2.triggerHeartbeat();
    c2.waitForHeartbeat(5000);

    // simulate node failure
    localCluster.failContainer(c0);

    // replacement container starts empty
    // nodes will deploy after downstream node was removed
    LocalStramChild c0Replaced = waitForContainer(localCluster, node1);
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

    Assert.assertEquals("downstream nodes after redeploy " + c2.getNodes(), 1, c2.getNodes().size());
    // verify that the downstream node was replaced
    Module n2Replaced = c2NodeMap.get(localCluster.findByLogicalNode(node2).id);
    Assert.assertNotNull(n2Replaced);
    Assert.assertNotSame("node2 redeployed", n2, n2Replaced);

    Module n1Replaced = nodeMap.get(ptNode1.id);
    Assert.assertNotNull(n1Replaced);

    ModuleContext n1ReplacedContext = c0.getNodeContext(ptNode1.id);
    Assert.assertEquals("initial window id", 1, n1ReplacedContext.getLastProcessedWindowId());

    localCluster.shutdown();
  }

  /**
   * Wait until instance of node comes online in a container
   *
   * @param localCluster
   * @param nodeConf
   * @return
   * @throws InterruptedException
   */
  private LocalStramChild waitForContainer(StramLocalCluster localCluster, Operator nodeDecl) throws InterruptedException
  {
    PTNode node = localCluster.findByLogicalNode(nodeDecl);
    Assert.assertNotNull("no node for " + nodeDecl, node);

    LocalStramChild container;
    while (true) {
      if (node.container.containerId != null) {
        if ((container = localCluster.getContainer(node.container.containerId)) != null) {
          if (container.getNodes().get(node.id) != null) {
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

  private void waitForWindow(ModuleContext nodeCtx, long windowId) throws InterruptedException
  {
    while (nodeCtx.getLastProcessedWindowId() < windowId) {
      LOG.debug("Waiting for window {} at node {}", windowId, nodeCtx.getId());
      Thread.sleep(100);
    }
  }

  private void backupNode(StramChild c, ModuleContext nodeCtx)
  {
    StramToNodeRequest backupRequest = new StramToNodeRequest();
    backupRequest.setNodeId(nodeCtx.getId());
    backupRequest.setRequestType(RequestType.CHECKPOINT);
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.nodeRequests = Collections.singletonList(backupRequest);
    LOG.debug("Requesting backup {} {}", c.getContainerId(), nodeCtx);
    c.processHeartbeatResponse(rsp);
  }

}

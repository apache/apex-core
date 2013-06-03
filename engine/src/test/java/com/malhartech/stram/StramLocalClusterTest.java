/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.codec.DefaultStatefulStreamCodec;
import com.malhartech.engine.*;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StramLocalCluster.MockComponentFactory;
import com.malhartech.stram.plan.logical.LogicalPlan;
import com.malhartech.stram.support.ManualScheduledExecutorService;
import com.malhartech.stram.support.StramTestSupport;
import com.malhartech.stream.BufferServerSubscriber;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import static java.lang.Thread.sleep;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StramLocalClusterTest
{
  private static final Logger LOG = LoggerFactory.getLogger(StramLocalClusterTest.class);

  @Before
  public void setup() throws IOException
  {
//    StramChild.eventloop = new DefaultEventLoop("StramLocalClusterTestEventLoop");
//    StramChild.eventloop.start();
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
    LogicalPlan dag = new LogicalPlan();

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

    dag.getAttributes().attr(LogicalPlan.STRAM_MAX_CONTAINERS).set(2);

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

  private static class TestBufferServerSubscriber
  {
    final BufferServerSubscriber bss;
    final StreamContext streamContext;
    final TestSink sink;
    SweepableReservoir reservoir;

    TestBufferServerSubscriber(PTOperator publisherOperator, String publisherPortName)
    {
      // sink to collect tuples emitted by the input module
      sink = new TestSink();
      String streamName = "testSinkStream";
      String sourceId = Integer.toString(publisherOperator.getId()).concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(publisherPortName);
      streamContext = new StreamContext(streamName);
      streamContext.setSourceId(sourceId);
      streamContext.setSinkId(this.getClass().getSimpleName());
      streamContext.setFinishedWindowId(-1);
      streamContext.setBufferServerAddress(publisherOperator.container.bufferServerAddress);
      streamContext.attr(StreamContext.CODEC).set(new DefaultStatefulStreamCodec<Object>());
      streamContext.attr(StreamContext.EVENT_LOOP).set(StramChild.eventloop);
      bss = new BufferServerSubscriber(streamContext.getSinkId(), 1024);
      bss.setup(streamContext);
      reservoir = bss.acquireReservoir("testSink", 1024);
      reservoir.setSink(sink);
    }

    @SuppressWarnings("SleepWhileInLoop")
    List<Object> retrieveTuples(int expectedCount, long timeoutMillis) throws InterruptedException
    {
      bss.activate(streamContext);
      for (long l = 0; l < timeoutMillis; l += 20) {
        if (reservoir.sweep() != null) {
          reservoir.remove();
        }
        if (sink.getResultCount() >= expectedCount) {
          break;
        }
        sleep(20);
      }
      //LOG.debug("test sink activated");
      Assert.assertEquals("received " + sink.collectedTuples, expectedCount, sink.collectedTuples.size());
      List<Object> result = new ArrayList<Object>(sink.collectedTuples);

      bss.deactivate();
      sink.collectedTuples.clear();
      return result;
    }

  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testRecovery() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator node1 = dag.addOperator("node1", TestGeneratorInputOperator.class);
    // data will be added externally from test
    node1.setMaxTuples(0);

    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);

    dag.addStream("n1n2", node1.outport, node2.inport1);

    dag.validate();

    dag.getAttributes().attr(LogicalPlan.STRAM_CHECKPOINT_WINDOW_COUNT).set(2);

    final ManualScheduledExecutorService wclock = new ManualScheduledExecutorService(1);

    MockComponentFactory mcf = new MockComponentFactory()
    {
      @Override
      public WindowGenerator setupWindowGenerator()
      {
        WindowGenerator wingen = StramTestSupport.setupWindowGenerator(wclock);
        wingen.setCheckpointCount(2);
        return wingen;
      }

    };

    StramLocalCluster localCluster = new StramLocalCluster(dag, mcf);
    localCluster.setPerContainerBufferServer(true);
    localCluster.setHeartbeatMonitoringEnabled(false); // driven by test
    localCluster.runAsync();


    PTOperator ptNode1 = localCluster.findByLogicalNode(dag.getMeta(node1));
    PTOperator ptNode2 = localCluster.findByLogicalNode(dag.getMeta(node2));

    LocalStramChild c0 = StramTestSupport.waitForActivation(localCluster, ptNode1);
    Map<Integer, Node<?>> nodeMap = c0.getNodes();
    Assert.assertEquals("number operators", 1, nodeMap.size());
    TestGeneratorInputOperator n1 = (TestGeneratorInputOperator)nodeMap.get(ptNode1.getId()).getOperator();
    Assert.assertNotNull(n1);

    LocalStramChild c2 = StramTestSupport.waitForActivation(localCluster, ptNode2);
    Map<Integer, Node<?>> c2NodeMap = c2.getNodes();
    Assert.assertEquals("number operators downstream", 1, c2NodeMap.size());
    GenericTestOperator n2 = (GenericTestOperator)c2NodeMap.get(localCluster.findByLogicalNode(dag.getMeta(node2)).getId()).getOperator();
    Assert.assertNotNull(n2);

    // input data
    String window0Tuple = "window0Tuple";
    n1.addTuple(window0Tuple);

    OperatorContext n1Context = c0.getNodeContext(ptNode1.getId());
    Assert.assertEquals("initial window id", 0, n1Context.getLastProcessedWindowId());
    wclock.tick(1); // begin window 1
    wclock.tick(1); // begin window 2
    StramTestSupport.waitForWindowComplete(n1Context, 1);

    wclock.tick(1); // begin window 3
    StramTestSupport.waitForWindowComplete(n1Context, 2);

    OperatorContext n2Context = c2.getNodeContext(ptNode2.getId());
    Assert.assertNotNull("context " + ptNode2);

    wclock.tick(1); // begin window 4

    StramTestSupport.waitForWindowComplete(n2Context, 3);
    n2.setMyStringProperty("checkpoint3");

    // move window forward, wait until propagated to module,
    // to ensure backup at previous window end was processed
    wclock.tick(1); // begin window 5
    StramTestSupport.waitForWindowComplete(n2Context, 4);

    // propagate checkpoints to master
    c0.triggerHeartbeat();
    // wait for heartbeat cycle to complete
    c0.waitForHeartbeat(5000);
    Assert.assertEquals("checkpoint " + ptNode1, 3, ptNode1.getRecentCheckpoint());
    c2.triggerHeartbeat();
    //Thread.yield();
    Thread.sleep(1); // yield without using yield for heartbeat cycle
    c2.waitForHeartbeat(5000);
    Assert.assertEquals("checkpoint " + ptNode2, 3, ptNode2.getRecentCheckpoint());

    // activated test sink, verify tuple stored at buffer server
    // sink to collect tuples emitted by the input operator
    //TestBufferServerSubscriber sink = new TestBufferServerSubscriber(ptNode1, TestGeneratorInputOperator.OUTPUT_PORT);
    //List<Object> tuples = sink.retrieveTuples(1, 3000);
    //Assert.assertEquals("received " + tuples, 1, tuples.size());
    //Assert.assertEquals("received " + tuples, window0Tuple, tuples.get(0));

    // simulate node failure
    localCluster.failContainer(c0);

    // replacement container starts empty
    // operators will deploy after downstream operator was removed
    LocalStramChild c0Replaced = StramTestSupport.waitForActivation(localCluster, ptNode1);
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

    Assert.assertEquals(c2.getContainerId() + " operators after redeploy " + c2.getNodes(), 1, c2.getNodes().size());
    // verify downstream node was replaced in same container
    Assert.assertEquals("active " + ptNode2, c2, StramTestSupport.waitForActivation(localCluster, ptNode2));
    GenericTestOperator n2Replaced = (GenericTestOperator)c2NodeMap.get(localCluster.findByLogicalNode(dag.getMeta(node2)).getId()).getOperator();
    Assert.assertNotNull("redeployed " + ptNode2, n2Replaced);
    Assert.assertNotSame("new instance " + ptNode2, n2, n2Replaced);
    Assert.assertEquals("restored state " + ptNode2, n2.getMyStringProperty(), n2Replaced.getMyStringProperty());

    TestGeneratorInputOperator n1Replaced = (TestGeneratorInputOperator)c0Replaced.getNodes().get(ptNode1.getId()).getOperator();
    Assert.assertNotNull(n1Replaced);

    OperatorContext n1ReplacedContext = c0Replaced.getNodeContext(ptNode1.getId());
    Assert.assertNotNull("node active " + ptNode1, n1ReplacedContext);
    // should node context should reflect last processed window (the backup window)?
    //Assert.assertEquals("initial window id", 1, n1ReplacedContext.getLastProcessedWindowId());
    wclock.tick(1);
    StramTestSupport.waitForWindowComplete(n1ReplacedContext, 5);

    // refresh n2 context after operator was re-deployed
    n2Context = c2.getNodeContext(ptNode2.getId());
    Assert.assertNotNull("node active " + ptNode2, n2Context);

    StramTestSupport.waitForWindowComplete(n2Context, 5);
    //backupNode(c0Replaced, n1ReplacedContext.getId()); // backup window 6
    //backupNode(c2, n2Context.getId()); // backup window 6
    wclock.tick(1); // end window 6

    StramTestSupport.waitForWindowComplete(n1ReplacedContext, 6);
    StramTestSupport.waitForWindowComplete(n2Context, 6);

    // propagate checkpoints to master
    c0Replaced.triggerHeartbeat();
    c0Replaced.waitForHeartbeat(5000);
    c2.triggerHeartbeat();
    c2.waitForHeartbeat(5000);

    String window6Tuple = "window6Tuple";
    n1Replaced.addTuple(window6Tuple);

    // reconnect as buffer was replaced
    //sink = new TestBufferServerSubscriber(ptNode1, TestGeneratorInputOperator.OUTPUT_PORT);
    // verify tuple sent before publisher checkpoint was removed from buffer during recovery
    // (publisher to resume from checkpoint id)
    //tuples = sink.retrieveTuples(1, 3000);
    //Assert.assertEquals("received " + tuples, 1, tuples.size());
    //Assert.assertEquals("received " + tuples, window6Tuple, tuples.get(0));

    // purge checkpoints
    localCluster.dnmgr.monitorHeartbeat(); // checkpoint purging

    Assert.assertEquals("checkpoints " + ptNode1, Arrays.asList(new Long[] {5L}), ptNode1.checkpointWindows);
    Assert.assertEquals("checkpoints " + ptNode2, Arrays.asList(new Long[] {5L}), ptNode2.checkpointWindows);

    //sink = new TestBufferServerSubscriber(ptNode1, TestGeneratorInputOperator.OUTPUT_PORT);
    // buffer server data purged
    //tuples = sink.retrieveTuples(1, 3000);
    //Assert.assertEquals("received " + tuples, 1, tuples.size());

    localCluster.shutdown();
  }

}

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.bufferserver.Server;
import com.malhartech.stram.conf.NewTopologyBuilder;
import com.malhartech.stram.conf.Topology;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StramChildTest
{
  public static final Logger logger = LoggerFactory.getLogger(StramChildTest.class);
  private static int bufferServerPort = 0;
  private static Server bufferServer = null;

  @BeforeClass
  public static void setup() throws InterruptedException, IOException, Exception
  {
    bufferServer = new Server(0); // find random port
    InetSocketAddress bindAddr = (InetSocketAddress)bufferServer.run();
    bufferServerPort = bindAddr.getPort();
  }

  @AfterClass
  public static void tearDown() throws IOException
  {
    if (bufferServer != null) {
      bufferServer.shutdown();
    }
  }

  public StramChildTest()
  {
  }

  /**
   * Instantiate physical model with adapters and partitioning in mock container.
   *
   * @throws Exception
   */
  @Test
  public void testInit() throws Exception
  {
    NewTopologyBuilder b = new NewTopologyBuilder();

    Topology.NodeDecl generatorNode = b.addNode("generatorNode", NumberGeneratorInputAdapter.class);
    Topology.NodeDecl node1 = b.addNode("node1", GenericTestNode.class);

    NewTopologyBuilder.StreamBuilder generatorOutput = b.addStream("generatorOutput");
    generatorOutput.setSource(generatorNode.getOutput(NumberGeneratorInputAdapter.OUTPUT_PORT))
            .addSink(node1.getInput(GenericTestNode.INPUT1))
            .setSerDeClass(DNodeManagerTest.TestStaticPartitioningSerDe.class);

    //StreamConf output1 = b.getOrAddStream("output1");
    //output1.addProperty(TopologyBuilder.STREAM_CLASSNAME,
    //                    ConsoleOutputStream.class.getName());
    Topology tplg = b.getTopology();

    DNodeManager dnm = new DNodeManager(tplg);
    int expectedContainerCount = DNodeManagerTest.TestStaticPartitioningSerDe.partitions.length;
    Assert.assertEquals("number required containers",
                        expectedContainerCount,
                        dnm.getNumRequiredContainers());

    List<StramLocalCluster.LocalStramChild> containers = new ArrayList<StramLocalCluster.LocalStramChild>();

    for (int i = 0; i < expectedContainerCount; i++) {
      String containerId = "container" + (i + 1);
      StreamingNodeUmbilicalProtocol.StreamingContainerContext cc = dnm.assignContainerForTest(containerId, InetSocketAddress.createUnresolved("localhost", bufferServerPort));
      StramLocalCluster.LocalStramChild container = new StramLocalCluster.LocalStramChild(containerId, null, null);
      container.init(cc);
      containers.add(container);
    }

    // TODO: validate data flow

    for (StramLocalCluster.LocalStramChild cc: containers) {
      logger.info("shutting down " + cc.getContainerId());
      cc.shutdown();
    }

    containers = null;
  }
}

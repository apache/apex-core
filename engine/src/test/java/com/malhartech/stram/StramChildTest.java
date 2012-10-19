/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.DAG;
import com.malhartech.bufferserver.Server;
import com.malhartech.dag.GenericTestModule;
import com.malhartech.dag.TestGeneratorInputModule;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StramChildTest
{
  private static final Logger logger = LoggerFactory.getLogger(StramChildTest.class);
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
    DAG dag = new DAG();

    TestGeneratorInputModule generator = dag.addOperator("StramChildTest.generator", TestGeneratorInputModule.class);
    GenericTestModule operator1 = dag.addOperator("operator1", GenericTestModule.class);

    DAG.StreamDecl generatorOutput = dag.addStream("generatorOutput");
    generatorOutput.setSource(generator.outport)
            .addSink(operator1.inport1)
            .setSerDeClass(StreamingContainerManagerTest.TestStaticPartitioningSerDe.class);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    int expectedContainerCount = StreamingContainerManagerTest.TestStaticPartitioningSerDe.partitions.length;
    Assert.assertEquals("number required containers",
                        expectedContainerCount,
                        dnm.getNumRequiredContainers());

    List<StramLocalCluster.LocalStramChild> containers = new ArrayList<StramLocalCluster.LocalStramChild>();

    for (int i = 0; i < expectedContainerCount; i++) {
      String containerId = "container" + (i + 1);
      StreamingContainerUmbilicalProtocol.StreamingContainerContext cc = dnm.assignContainerForTest(containerId, InetSocketAddress.createUnresolved("localhost", bufferServerPort));
      StramLocalCluster.LocalStramChild container = new StramLocalCluster.LocalStramChild(containerId, null, null);
      container.setup(cc);
      containers.add(container);
    }

    // TODO: validate data flow

    for (StramLocalCluster.LocalStramChild cc: containers) {
      logger.info("shutting down " + cc.getContainerId());
      cc.teardown();
    }
  }
}

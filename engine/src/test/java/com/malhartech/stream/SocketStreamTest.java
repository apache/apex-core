/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.Server;
import com.malhartech.dag.*;
import com.malhartech.stram.DNodeManagerTest.TestStaticPartitioningSerDe;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.*;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SocketStreamTest
{
  private static Logger LOG = LoggerFactory.getLogger(SocketStreamTest.class);
  private static int bufferServerPort = 0;
  private static Server bufferServer = null;

  @BeforeClass
  public static void setup() throws InterruptedException, IOException
  {
    //   java.util.logging.Logger.getLogger("").setLevel(java.util.logging.Level.FINEST);
    //    java.util.logging.Logger.getLogger("").info("test");
    bufferServer = new Server(0); // find random port
    InetSocketAddress bindAddr = (InetSocketAddress) bufferServer.run();
    bufferServerPort = bindAddr.getPort();

  }

  @AfterClass
  public static void tearDown() throws IOException
  {
    if (bufferServer != null) {
      bufferServer.shutdown();
    }
  }

  /**
   * Send tuple on outputstream and receive tuple from inputstream
   *
   * @throws Exception
   */
  @Test
  public void testBufferServerStream() throws Exception
  {

    final AtomicInteger messageCount = new AtomicInteger();
    Sink sink = new Sink()
    {
      @Override
      public void doSomething(Tuple t)
      {
        switch (t.getType()) {
          case BEGIN_WINDOW:
            break;

          case END_WINDOW:
            synchronized (SocketStreamTest.this) {
              SocketStreamTest.this.notifyAll();
            }
            break;

          case SIMPLE_DATA:
            System.out.println("received: " + t.getObject());
            messageCount.incrementAndGet();
        }
      }
    };

    SerDe serde = new DefaultSerDe();

    String streamName = "streamName"; // AKA "type"
    String upstreamNodeId = "upstreamNodeId";
    String downstreamNodeId = "downStreamNodeId";


    BufferServerStreamContext issContext = new BufferServerStreamContext();
    issContext.setSink(sink);
    issContext.setSerde(serde);
    issContext.setId(streamName);
    issContext.setSourceId(upstreamNodeId);
    issContext.setSinkId(downstreamNodeId);


    StreamConfiguration sconf = new StreamConfiguration(Collections.<String, String>emptyMap());
    sconf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved("localhost", bufferServerPort));

    BufferServerInputStream iss = new BufferServerInputStream();
    iss.setup(sconf);
    iss.setContext(issContext);

    BufferServerStreamContext ossContext = new BufferServerStreamContext();
    ossContext.setSerde(serde);
    ossContext.setId(streamName);
    ossContext.setSourceId(upstreamNodeId);
    ossContext.setSinkId(downstreamNodeId);

    BufferServerOutputStream oss = new BufferServerOutputStream();
    oss.setup(sconf);
    oss.setContext(ossContext);
    // oss.setContext(ossContext, upstreamNodeId, streamName);

    oss.activate();
    LOG.debug("output stream activated");
    iss.activate();
    LOG.debug("input stream activated");


    LOG.info("Sending hello message");
    oss.doSomething(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 0, ossContext));
    oss.doSomething(StramTestSupport.generateTuple("hello", 0, ossContext));
    oss.doSomething(StramTestSupport.generateEndWindowTuple(upstreamNodeId, 0, 1, ossContext));
    oss.doSomething(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 1, ossContext));
    synchronized (SocketStreamTest.this) {
      if (messageCount.get() == 0) { // don't wait if already notified
        SocketStreamTest.this.wait(2000);
      }
    }

    Assert.assertEquals("Received messages", 1, messageCount.get());
    System.out.println("exiting...");

  }

  private class ChildContainer extends StramChild
  {
    public ChildContainer(String containerId)
    {
      super(containerId, new Configuration(), null);
    }

    private void initForTest(StreamingContainerContext ctx) throws IOException
    {
      super.init(ctx);
    }

    @Override
    public void shutdown()
    {
      super.shutdown();
    }
  }

  /**
   * Instantiate physical model with adapters and partitioning in mock container.
   *
   * @throws Exception
   */
  @Test
  public void testStramChildInit() throws Exception
  {
    AdapterWrapperNode wn = new AdapterWrapperNode(null);
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(AdapterWrapperNode.KEY_IS_INPUT, "true");
    BeanUtils.populate(wn, properties);
    Assert.assertTrue(wn.isInput());

    TopologyBuilder b = new TopologyBuilder(new Configuration());

    NodeConf node1 = b.getOrAddNode("node1");

    StreamConf input1 = b.getOrAddStream("input1");
    input1.addProperty(TopologyBuilder.STREAM_CLASSNAME,
                       NumberGeneratorInputAdapter.class.getName());
    input1.addProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME,
                       TestStaticPartitioningSerDe.class.getName());


    StreamConf output1 = b.getOrAddStream("output1");
    output1.addProperty(TopologyBuilder.STREAM_CLASSNAME,
                        ConsoleOutputStream.class.getName());

    node1.addInput(input1);
    node1.addOutput(output1);

    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    DNodeManager dnm = new DNodeManager(b);
    int expectedContainerCount = TestStaticPartitioningSerDe.partitions.length;
    Assert.assertEquals("number required containers",
                        expectedContainerCount,
                        dnm.getNumRequiredContainers());

    List<ChildContainer> containers = new ArrayList<ChildContainer>();

    for (int i = 0; i < expectedContainerCount; i++) {
      String containerId = "container" + (i + 1);
      StreamingContainerContext cc = dnm.assignContainer(containerId, InetSocketAddress.createUnresolved("localhost", bufferServerPort));
      ChildContainer container = new ChildContainer(containerId);
      container.initForTest(cc);
      containers.add(container);
    }

    // TODO: validate data flow

    for (ChildContainer cc : containers) {
      LOG.info("shutting down " + cc);
      cc.shutdown();
    }

    containers = null;
  }
}

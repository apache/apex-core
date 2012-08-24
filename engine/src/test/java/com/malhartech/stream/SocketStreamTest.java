/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.Server;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.SerDe;
import com.malhartech.dag.Sink;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.Tuple;
import com.malhartech.stram.AdapterWrapperNode;
import com.malhartech.stram.DNodeManager;
import com.malhartech.stram.DNodeManagerTest.TestStaticPartitioningSerDe;
import com.malhartech.stram.NumberGeneratorInputAdapter;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.TopologyBuilderTest;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.beanutils.BeanUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
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


        BufferServerStreamContext issContext = new BufferServerStreamContext(upstreamNodeId, downstreamNodeId);
        issContext.setSink(sink);
        issContext.setSerde(serde);
        issContext.setId(streamName);

        StreamConfiguration sconf = new StreamConfiguration(Collections.<String, String>emptyMap());
        sconf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved("localhost", bufferServerPort));

        BufferServerInputStream iss = new BufferServerInputStream();
        iss.setup(sconf);
  //      iss.setContext(issContext);

        BufferServerStreamContext ossContext = new BufferServerStreamContext(upstreamNodeId, downstreamNodeId);
        ossContext.setSerde(serde);
        ossContext.setId(streamName);

        BufferServerOutputStream oss = new BufferServerOutputStream();
        oss.setup(sconf);
//        oss.setContext(ossContext);

        oss.activate(ossContext);
        LOG.debug("output stream activated");
        iss.activate(issContext);
        LOG.debug("input stream activated");


        LOG.debug("Sending hello message");
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

    /**
     * Test to verify the adapter wrapper node initialization from properties.
     *
     * @throws Exception
     */
    @Test
    public void testAdapterWrapperNodeInit() throws Exception
    {
        AdapterWrapperNode wn = new AdapterWrapperNode();
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(AdapterWrapperNode.KEY_IS_INPUT, "true");
        properties.put(AdapterWrapperNode.CHECKPOINT_WINDOW_ID, "999");
        BeanUtils.populate(wn, properties);
        Assert.assertTrue(wn.isInput());
        Assert.assertEquals(999, wn.getCheckPointWindowId());
    }

    /**
     * Instantiate physical model with adapters and partitioning in mock container.
     *
     * @throws Exception
     */
    @Test
    public void testStramChildInit() throws Exception
    {
        TopologyBuilder b = new TopologyBuilder();

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

        for (NodeConf nodeConf: b.getAllNodes().values()) {
            nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
        }

        DNodeManager dnm = new DNodeManager(b);
        int expectedContainerCount = TestStaticPartitioningSerDe.partitions.length;
        Assert.assertEquals("number required containers",
                            expectedContainerCount,
                            dnm.getNumRequiredContainers());

        List<LocalStramChild> containers = new ArrayList<LocalStramChild>();

        for (int i = 0; i < expectedContainerCount; i++) {
            String containerId = "container" + (i + 1);
            StreamingContainerContext cc = dnm.assignContainerForTest(containerId, InetSocketAddress.createUnresolved("localhost", bufferServerPort));
            LocalStramChild container = new LocalStramChild(containerId, null);
            container.init(cc);
            containers.add(container);
        }

        // TODO: validate data flow

        for (LocalStramChild cc: containers) {
            LOG.info("shutting down " + cc);
            cc.shutdown();
        }

        containers = null;
    }
}

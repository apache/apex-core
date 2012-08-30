/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.bufferserver.Server;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.SerDe;
import com.malhartech.dag.Sink;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.Tuple;
import com.malhartech.stram.DNodeManager;
import com.malhartech.stram.DNodeManagerTest.TestStaticPartitioningSerDe;
import com.malhartech.stram.NumberGeneratorInputAdapter;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.TopologyBuilderTest;
import com.malhartech.stram.TopologyBuilderTest.EchoNode;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;

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
     * Test buffer server stream by sending
     * tuple on outputstream and receive same tuple from inputstream
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
            public void process(Object payload)
            {
              if (payload instanceof Tuple) {
                Tuple t = (Tuple)payload;
                switch (t.getType()) {
                    case BEGIN_WINDOW:
                        break;

                    case END_WINDOW:
                        synchronized (SocketStreamTest.this) {
                            SocketStreamTest.this.notifyAll();
                        }
                        break;
                }
              } else {
                  System.out.println("received: " + payload);
                  messageCount.incrementAndGet();
              }
            }
        };

        SerDe serde = new DefaultSerDe();

        String streamName = "streamName"; // AKA "type"
        String upstreamNodeId = "upstreamNodeId";
        String downstreamNodeId = "downStreamNodeId";


        BufferServerStreamContext issContext = new BufferServerStreamContext(streamName);
        issContext.setSourceId(upstreamNodeId);
        issContext.setSinkId(downstreamNodeId);
        issContext.setSerde(serde);

        StreamConfiguration sconf = new StreamConfiguration(Collections.<String, String>emptyMap());
        sconf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved("localhost", bufferServerPort));

        BufferServerInputStream iss = new BufferServerInputStream();
        iss.setup(sconf);
        iss.connect("testSink", sink);

        BufferServerStreamContext ossContext = new BufferServerStreamContext(streamName);
        ossContext.setSourceId(upstreamNodeId);
        ossContext.setSinkId(downstreamNodeId);
        ossContext.setSerde(serde);

        BufferServerOutputStream oss = new BufferServerOutputStream();
        oss.setup(sconf);

        oss.activate(ossContext);
        LOG.debug("output stream activated");
        iss.activate(issContext);
        LOG.debug("input stream activated");


        LOG.debug("Sending hello message");
        oss.process(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 0));
        oss.process(StramTestSupport.generateTuple("hello", 0));
        oss.process(StramTestSupport.generateEndWindowTuple(upstreamNodeId, 0, 1));
        oss.process(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 1));
        synchronized (SocketStreamTest.this) {
            if (messageCount.get() == 0) { // don't wait if already notified
                SocketStreamTest.this.wait(2000);
            }
        }

        Assert.assertEquals("Received messages", 1, messageCount.get());
        System.out.println("exiting...");

    }

    /**
     * Instantiate physical model with adapters and partitioning in mock container.
     *
     * @throws Exception
     */
    @Ignore
    @Test
    public void testStramChildInit() throws Exception
    {
        TopologyBuilder b = new TopologyBuilder();

        NodeConf generatorNode = b.getOrAddNode("generatorNode");
        generatorNode.setClassName(NumberGeneratorInputAdapter.class.getName());

        NodeConf node1 = b.getOrAddNode("node1");
        node1.setClassName(TopologyBuilderTest.EchoNode.class.getName());

        StreamConf generatorOutput = b.getOrAddStream("generatorOutput");
        generatorOutput.setSource(NumberGeneratorInputAdapter.OUTPUT_PORT, generatorNode)
          .addSink(EchoNode.INPUT1, node1)
          .addProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME, TestStaticPartitioningSerDe.class.getName());

        //StreamConf output1 = b.getOrAddStream("output1");
        //output1.addProperty(TopologyBuilder.STREAM_CLASSNAME,
        //                    ConsoleOutputStream.class.getName());
        Topology tplg = b.getTopology();

        DNodeManager dnm = new DNodeManager(tplg);
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
            LOG.info("shutting down " + cc.getContainerId());
            cc.shutdown();
        }

        containers = null;
    }
}

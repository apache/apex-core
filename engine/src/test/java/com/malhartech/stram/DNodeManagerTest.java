/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;

public class DNodeManagerTest {

  @Test
  public void testAssignContainer() {

    TopologyBuilder b = new TopologyBuilder(new Configuration());
    
    NodeConf node1 = b.getOrAddNode("node1");
    NodeConf node2 = b.getOrAddNode("node2");
    NodeConf node3 = b.getOrAddNode("node3");

    node1.addOutput(b.getOrAddStream("n1n2"));
    node2.addInput(b.getOrAddStream("n1n2"));

    node2.addOutput(b.getOrAddStream("n2n3"));
    node3.addInput(b.getOrAddStream("n2n3"));
    b.getOrAddStream("n2n3").addProperty(TopologyBuilder.STREAM_INLINE, "true");
    
    Assert.assertEquals("number nodes", 3, b.getAllNodes().values().size());
    Assert.assertEquals("number root nodes", 1, b.getRootNodes().size());

    for (NodeConf nodeConf : b.getAllNodes().values()) {
        // required to construct context
        nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    DNodeManager dnm = new DNodeManager(b);
    Assert.assertEquals("number required containers", 2, dnm.getNumRequiredContainers());
    
    String container1Id = "container1";
    String container2Id = "container2";
    
    // node1 needs to be deployed first, regardless in which order they were given
    StreamingContainerContext c1 = dnm.assignContainer(container1Id, InetSocketAddress.createUnresolved(container1Id+"Host", 9001));
    Assert.assertEquals("one node assigned to container", 1, c1.getNodes().size());
    Assert.assertTrue(node1.getId() + " assigned to " + container1Id, containsNodeContext(c1, node1));
    Assert.assertEquals("one stream connection for container1", 1, c1.getStreams().size());
    StreamContext c1n1n2 = getStreamContext(c1, "n1n2");
    Assert.assertNotNull("one stream connection for container1", c1n1n2);
    Assert.assertEquals("stream connects to upstream host", container1Id + "Host", c1n1n2.getBufferServerHost());
    Assert.assertEquals("stream connects to upstream port", 9001, c1n1n2.getBufferServerPort());
    
    StreamingContainerContext c2 = dnm.assignContainer(container2Id, InetSocketAddress.createUnresolved(container2Id+"Host", 9002));
    Assert.assertEquals("one node assigned to container", 2, c2.getNodes().size());
    Assert.assertTrue(node2.getId() + " assigned to " + container2Id, containsNodeContext(c2, node2));
    Assert.assertTrue(node3.getId() + " assigned to " + container2Id, containsNodeContext(c2, node3));
    
    Assert.assertEquals("one stream connection for container2", 2, c2.getStreams().size());
    StreamContext c2n1n2 = getStreamContext(c2, "n1n2");
    Assert.assertNotNull("stream connection for container2", c2n1n2);
    Assert.assertEquals("stream connects to upstream host", container1Id + "Host", c2n1n2.getBufferServerHost());
    Assert.assertEquals("stream connects to upstream port", 9001, c2n1n2.getBufferServerPort());
    
  }

  private boolean containsNodeContext(StreamingContainerContext scc, NodeConf nodeConf) {
    for (StreamingNodeContext snc : scc.getNodes()) {
      if (nodeConf.getId().equals(snc.getLogicalId())) {
        return true;
      }
    }
    return false;
  }

  private static StreamContext getStreamContext(StreamingContainerContext scc, String streamId) {
    for (StreamContext sc : scc.getStreams()) {
      if (streamId.equals(sc.getId())) {
        return sc;
      }
    }
    return null;
  }
  
}

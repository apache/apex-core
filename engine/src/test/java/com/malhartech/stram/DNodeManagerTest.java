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

    DNodeManager dnm = new DNodeManager();

    Assert.assertEquals("number nodes", 3, b.getAllNodes().values().size());
    Assert.assertEquals("number root nodes", 1, b.getRootNodes().size());
    
    dnm.addNodes(b.getAllNodes().values());
    for (NodeConf nodeConf : b.getAllNodes().values()) {
        // required to construct context
        nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }
    String container1Id = "container1";
    String container2Id = "container2";
    
    StreamingContainerContext c1 = dnm.assignContainer(container1Id, new InetSocketAddress(0));
    Assert.assertEquals("one node assigned to container", 1, c1.getNodes().size());
    Assert.assertEquals("first node assigned to container", "node1", c1.getNodes().get(0).getLogicalId());
    Assert.assertEquals("one stream connection for container1", "n1n2", c1.getStreams().get(0).getId());
    
    StreamingContainerContext c2 = dnm.assignContainer(container2Id, new InetSocketAddress(0));
    Assert.assertEquals("one node assigned to container", 1, c2.getNodes().size());
    Assert.assertEquals("first node assigned to container", "node2", c2.getNodes().get(0).getLogicalId());
    Assert.assertEquals("one stream connection for container2", "n1n2", c2.getStreams().get(0).getId());
    
  }

}

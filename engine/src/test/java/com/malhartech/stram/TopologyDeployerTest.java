/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.stram.DNodeManagerTest.TestStaticPartitioningSerDe;
import com.malhartech.stram.TopologyDeployer.PTNode;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;

public class TopologyDeployerTest {

  @Test
  public void testStaticPartitioning() {
    TopologyBuilder b = new TopologyBuilder(new Configuration());
    
    NodeConf node1 = b.getOrAddNode("node1");
    NodeConf node2 = b.getOrAddNode("node2");

    NodeConf mergeNode = b.getOrAddNode("mergeNode");
    
    StreamConf n1n2 = b.getOrAddStream("n1n2");
    n1n2.addProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME, TestStaticPartitioningSerDe.class.getName());
    
    node1.addOutput(n1n2);
    node2.addInput(n1n2);

    StreamConf mergeStream = b.getOrAddStream("mergeStream");
    node2.addOutput(mergeStream);
    mergeNode.addInput(mergeStream);
    
    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    TopologyDeployer td = new TopologyDeployer();
    td.init(2, b);
    
    Assert.assertEquals("number of containers", 2, td.getContainers().size());
  }  

  @Test
  public void testInline() {
    
    TopologyBuilder b = new TopologyBuilder(new Configuration());
    
    NodeConf node1 = b.getOrAddNode("node1");
    NodeConf node2 = b.getOrAddNode("node2");
    NodeConf node3 = b.getOrAddNode("node3");
    
    NodeConf notInlineNode = b.getOrAddNode("notInlineNode");
    NodeConf partNode = b.getOrAddNode("partNode");
    
    StreamConf n1n2 = b.getOrAddStream("n1n2");
    n1n2.addProperty(TopologyBuilder.STREAM_INLINE, String.valueOf(true));
    node1.addOutput(n1n2);
    node2.addInput(n1n2);

    // node 3 has 2 inputs, one of them inline
    StreamConf n1n3 = b.getOrAddStream("n1n3");
    n1n3.addProperty(TopologyBuilder.STREAM_INLINE, String.valueOf(true));
    node1.addOutput(n1n3);
    node3.addInput(n1n3);

    StreamConf n2n3 = b.getOrAddStream("n2n3");
    n2n3.addProperty(TopologyBuilder.STREAM_INLINE, String.valueOf(false));
    node2.addOutput(n2n3);
    node3.addInput(n2n3);
    
    
    StreamConf notInlineStream = b.getOrAddStream("notInlineStream");
    node2.addOutput(notInlineStream);
    notInlineNode.addInput(notInlineStream);
    
    // partNode has 2 inputs, inline must be ignored with partitioned input
    StreamConf n3toPart = b.getOrAddStream("n3toPart");
    n3toPart.addProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME, TestStaticPartitioningSerDe.class.getName());
    node3.addOutput(n3toPart);
    partNode.addInput(n3toPart);

    StreamConf n2toPart = b.getOrAddStream("n2toPart");
    n1n2.addProperty(TopologyBuilder.STREAM_INLINE, String.valueOf(true));
    node2.addOutput(n2toPart);
    partNode.addInput(n2toPart);
    
    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    int maxContainers = 5;
    TopologyDeployer deployer1 = new TopologyDeployer();
    deployer1.init(maxContainers, b);
    Assert.assertEquals("number of containers", maxContainers, deployer1.getContainers().size());
    Assert.assertEquals("nodes container 0", 3, deployer1.getContainers().get(0).nodes.size());
    List<NodeConf> c1ExpNodes = Arrays.asList(node1, node2, node3);
    List<NodeConf> c1ActNodes = new ArrayList<NodeConf>();
    for (PTNode pNode : deployer1.getContainers().get(0).nodes) {
      c1ActNodes.add(pNode.getLogicalNode());
    }
    Assert.assertEquals("nodes container 0", c1ExpNodes, c1ActNodes);
    
    Assert.assertEquals("nodes container 1", 1, deployer1.getContainers().get(1).nodes.size());
    Assert.assertEquals("nodes container 1", notInlineNode, deployer1.getContainers().get(1).nodes.get(0).getLogicalNode());

    // one container per partition
    for (int cindex = 2; cindex < maxContainers; cindex++) {
      Assert.assertEquals("nodes container" + cindex, 1, deployer1.getContainers().get(cindex).nodes.size());
      Assert.assertEquals("nodes container" + cindex, partNode, deployer1.getContainers().get(cindex).nodes.get(0).getLogicalNode());
    }
    
  }  
  
}

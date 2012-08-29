/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.net.InetSocketAddress;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.dag.DefaultSerDe;
import com.malhartech.stram.NodeDeployInfo.NodeInputDeployInfo;
import com.malhartech.stram.NodeDeployInfo.NodeOutputDeployInfo;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.TopologyBuilderTest.EchoNode;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;

public class DNodeManagerTest {

  @Test
  public void testAssignContainer() {

    TopologyBuilder b = new TopologyBuilder(new Configuration());

    NodeConf node1 = b.getOrAddNode("node1");
    NodeConf node2 = b.getOrAddNode("node2");
    NodeConf node3 = b.getOrAddNode("node3");
    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    b.getOrAddStream("n1n2")
      .setSource(EchoNode.OUTPUT1, node1)
      .addSink(EchoNode.INPUT1, node2);

    b.getOrAddStream("n2n3")
      .addProperty(TopologyBuilder.STREAM_INLINE, "true")
      .setSource(EchoNode.OUTPUT1, node2)
      .addSink(EchoNode.INPUT1, node3);
    
    Assert.assertEquals("number nodes", 3, b.getAllNodes().values().size());
    Assert.assertEquals("number root nodes", 1, b.getRootNodes().size());

    Topology tplg = b.getTopology();
    tplg.setContainerCount(2);
    DNodeManager dnm = new DNodeManager(tplg);
    Assert.assertEquals("number required containers", 2, dnm.getNumRequiredContainers());
    
    String container1Id = "container1";
    String container2Id = "container2";
    
    // node1 needs to be deployed first, regardless in which order they were given
    StreamingContainerContext c1 = dnm.assignContainerForTest(container1Id, InetSocketAddress.createUnresolved(container1Id+"Host", 9001));
    Assert.assertEquals("number nodes assigned to c1", 1, c1.nodeList.size());
    NodeDeployInfo node1DI = getNodeDeployInfo(c1, node1);
    Assert.assertNotNull(node1.getId() + " assigned to " + container1Id, node1DI);
    Assert.assertEquals("inputs " + node1DI.declaredId, 0, node1DI.inputs.size());
    Assert.assertEquals("outputs " + node1DI.declaredId, 1, node1DI.outputs.size());
    Assert.assertNotNull("serializedNode " + node1DI.declaredId, node1DI.serializedNode);
    
    NodeOutputDeployInfo c1n1n2 = node1DI.outputs.get(0);
    Assert.assertNotNull("stream connection for container1", c1n1n2);
    Assert.assertEquals("stream connection for container1", "n1n2", c1n1n2.declaredStreamId);
    Assert.assertEquals("stream connects to upstream host", container1Id + "Host", c1n1n2.bufferServerHost);
    Assert.assertEquals("stream connects to upstream port", 9001, c1n1n2.bufferServerPort);
    Assert.assertFalse("stream inline", c1n1n2.isInline());

    StreamingContainerContext c2 = dnm.assignContainerForTest(container2Id, InetSocketAddress.createUnresolved(container2Id+"Host", 9002));
    Assert.assertEquals("number nodes assigned to container", 2, c2.nodeList.size());
    NodeDeployInfo node2DI = getNodeDeployInfo(c2, node2);
    NodeDeployInfo node3DI = getNodeDeployInfo(c2, node3);
    Assert.assertNotNull(node2.getId() + " assigned to " + container2Id, node2DI);
    Assert.assertNotNull(node3.getId() + " assigned to " + container2Id, node3DI);
    
    // buffer server input node2 from node1
    NodeInputDeployInfo c2n1n2 = getInputDeployInfo(node2DI, "n1n2");
    Assert.assertNotNull("stream connection for container2", c2n1n2);
    Assert.assertEquals("stream connects to upstream host", container1Id + "Host", c2n1n2.bufferServerHost);
    Assert.assertEquals("stream connects to upstream port", 9001, c2n1n2.bufferServerPort);
    Assert.assertEquals("portName " + c2n1n2, EchoNode.INPUT1, c2n1n2.portName);
    Assert.assertNull("partitionKeys " + c2n1n2, c2n1n2.partitionKeys);
    Assert.assertEquals("sourceNodeId " + c2n1n2, node1DI.id, c2n1n2.sourceNodeId);

    // inline input node3 from node2
    NodeInputDeployInfo c2n3In = getInputDeployInfo(node3DI, "n2n3");
    Assert.assertNotNull("input " + c2n3In, node2DI);
    Assert.assertEquals("portName " + c2n3In, EchoNode.INPUT1, c2n3In.portName);
    Assert.assertNotNull("stream connection for container2", c2n3In);
    Assert.assertNull("bufferServerHost " + c2n3In, c2n3In.bufferServerHost);
    Assert.assertEquals("bufferServerPort " + c2n3In, 0, c2n3In.bufferServerPort);
    Assert.assertNull("partitionKeys " + c2n3In, c2n3In.partitionKeys);
    Assert.assertEquals("sourceNodeId " + c2n3In, node2DI.id, c2n3In.sourceNodeId);
    Assert.assertEquals("sourcePortName " + c2n3In, EchoNode.OUTPUT1, c2n3In.sourcePortName); // required for inline
    
  }
/*
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
    
    b.setContainerCount(5);
    DNodeManager dnm = new DNodeManager(b);
    Assert.assertEquals("number required containers", 5, dnm.getNumRequiredContainers());

    String container1Id = "container1";
    StreamingContainerContext c1 = dnm.assignContainerForTest(container1Id, InetSocketAddress.createUnresolved(container1Id+"Host", 9001));
    Assert.assertEquals("number nodes assigned to container", 1, c1.getNodes().size());
    Assert.assertTrue(node2.getId() + " assigned to " + container1Id, containsNodeContext(c1, node1));

    for (int i=0; i<TestStaticPartitioningSerDe.partitions.length; i++) {
      String containerId = "container"+(i+1);
      StreamingContainerContext cc = dnm.assignContainerForTest(containerId, InetSocketAddress.createUnresolved(containerId+"Host", 9001));
      Assert.assertEquals("number nodes assigned to container", 1, cc.getNodes().size());
      Assert.assertTrue(node2.getId() + " assigned to " + containerId, containsNodeContext(cc, node2));
  
      // n1n2 in, mergeStream out
      Assert.assertEquals("stream connections for " + containerId, 2, cc.getStreams().size());
      StreamPConf sc = getStreamContext(cc, "n1n2");
      Assert.assertNotNull("stream connection for " + containerId, sc);
      Assert.assertTrue("partition for " + containerId, Arrays.equals(TestStaticPartitioningSerDe.partitions[i], sc.getPartitionKeys().get(0)));
    }
    
    // mergeNode container 
    String mergeContainerId = "mergeNodeContainer";
    StreamingContainerContext cmerge = dnm.assignContainerForTest(mergeContainerId, InetSocketAddress.createUnresolved(mergeContainerId+"Host", 9001));
    Assert.assertEquals("number nodes assigned to " + mergeContainerId, 1, cmerge.getNodes().size());
    Assert.assertTrue(mergeNode.getId() + " assigned to " + container1Id, containsNodeContext(cmerge, mergeNode));
    Assert.assertEquals("stream connections for " + mergeContainerId, 3, cmerge.getStreams().size());
    
  }  
  
  @Test
  public void testAdaptersWithStaticPartitioning() {
    TopologyBuilder b = new TopologyBuilder(new Configuration());

    NodeConf node1 = b.getOrAddNode("node1");
    
    StreamConf input1 = b.getOrAddStream("input1");
    input1.addProperty(TopologyBuilder.STREAM_CLASSNAME, NumberGeneratorInputAdapter.class.getName());
    input1.addProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME, TestStaticPartitioningSerDe.class.getName());

    StreamConf output1 = b.getOrAddStream("output1");
    output1.addProperty(TopologyBuilder.STREAM_CLASSNAME, NumberGeneratorInputAdapter.class.getName());
    
    node1.addInput(input1);
    node1.addOutput(output1);
    
    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    int expectedContainerCount = TestStaticPartitioningSerDe.partitions.length;
    b.setContainerCount(expectedContainerCount);
    DNodeManager dnm = new DNodeManager(b);

    Assert.assertEquals("number required containers", expectedContainerCount, dnm.getNumRequiredContainers());
    
    for (int i=0; i<expectedContainerCount; i++) {
      String containerId = "container"+(i+1);
      StreamingContainerContext cc = dnm.assignContainerForTest(containerId, InetSocketAddress.createUnresolved(containerId+"Host", 9001));

      NodePConf node1PNode = getNodeContext(cc, node1.getId());
      Assert.assertNotNull(node1.getId() + " assigned to " + containerId, node1PNode);
      
      if (i==0) {
        // the input and output adapter should be assigned to first container (deployed with first partition)
        Assert.assertEquals("number nodes assigned to " + containerId, 3, cc.getNodes().size());
        // output subscribes to all upstream partitions
        Assert.assertEquals("number streams " + containerId, 1+TestStaticPartitioningSerDe.partitions.length, cc.getStreams().size());
        
        NodePConf input1PNode = getNodeContext(cc, input1.getId());
        Assert.assertNotNull(input1.getId() + " assigned to " + containerId, input1PNode);
        
        NodePConf output1PNode = getNodeContext(cc, output1.getId());
        Assert.assertNotNull(output1.getId() + " assigned to " + containerId, output1PNode);
        
        int inlineStreamCount = 0;
        for (StreamPConf pstream : cc.getStreams()) {
          if (pstream.getTargetNodeId() == output1PNode.getDnodeId() && pstream.getSourceNodeId() == node1PNode.getDnodeId()) {
            Assert.assertTrue("stream from " + node1PNode + " to " + output1PNode + " inline", pstream.isInline());
            inlineStreamCount++;
          }
        }
        Assert.assertEquals("number inline streams", 1, inlineStreamCount);
        
      } else {
        Assert.assertEquals("number nodes assigned to " + containerId, 1, cc.getNodes().size());
        Assert.assertEquals("number streams " + containerId, 2, cc.getStreams().size());
      }
        
      StreamPConf scIn1 = getStreamContext(cc, "input1");
      Assert.assertNotNull("in stream connection for " + containerId, scIn1);
      Assert.assertTrue("partition for " + containerId, Arrays.equals(TestStaticPartitioningSerDe.partitions[i], scIn1.getPartitionKeys().get(0)));
      Assert.assertFalse(scIn1.isInline());
      
      StreamPConf scOut1 = getStreamContext(cc, "output1");
      Assert.assertNotNull("out stream connection for " + containerId, scOut1);
      
      if (i==0) {
        Assert.assertTrue(containerId +"/" + scOut1.getId() + " inline", scOut1.isInline()); // first stream inline
      } else {
        Assert.assertFalse(containerId +"/" + scOut1.getId() + " not inline", scOut1.isInline()); // first stream inline
      }
    }
  }  
*/
  public static class TestStaticPartitioningSerDe extends DefaultSerDe {

    public final static byte[][] partitions = new byte[][]{
        {'1'}, {'2'}, {'3'}
    };
    
    @Override
    public byte[][] getPartitions() {
      return partitions;
    }
  }
  
  private boolean containsNodeContext(StreamingContainerContext scc, NodeConf nodeConf) {
    return getNodeDeployInfo(scc, nodeConf) != null;
  }

  private static NodeDeployInfo getNodeDeployInfo(StreamingContainerContext scc, NodeConf nodeConf) {
    for (NodeDeployInfo ndi : scc.nodeList) {
      if (nodeConf.getId().equals(ndi.declaredId)) {
        return ndi;
      }
    }
    return null;
  }
  
  private static NodeInputDeployInfo getInputDeployInfo(NodeDeployInfo ndi, String streamId) {
    for (NodeInputDeployInfo in : ndi.inputs) {
      if (streamId.equals(in.declaredStreamId)) {
        return in;
      }
    }
    return null;
  }

}

/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.junit.Test;

import com.malhartech.dag.DefaultSerDe;
import com.malhartech.stram.NodeDeployInfo.NodeInputDeployInfo;
import com.malhartech.stram.NodeDeployInfo.NodeOutputDeployInfo;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.TopologyBuilderTest.EchoNode;
import com.malhartech.stram.TopologyDeployer.PTNode;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;

public class DNodeManagerTest {

  @Test
  public void testNodeDeployInfoSerialization() throws Exception {
    NodeDeployInfo ndi = new NodeDeployInfo();
    ndi.declaredId = "node1";
    ndi.id ="1";
    
    NodeDeployInfo.NodeInputDeployInfo input = new NodeDeployInfo.NodeInputDeployInfo();
    input.declaredStreamId = "streamToNode";
    input.portName = "inputPortNameOnNode";
    input.sourceNodeId = "sourceNodeId";

    ndi.inputs = new ArrayList<NodeDeployInfo.NodeInputDeployInfo>();
    ndi.inputs.add(input);

    NodeDeployInfo.NodeOutputDeployInfo output = new NodeDeployInfo.NodeOutputDeployInfo();
    output.declaredStreamId = "streamFromNode";
    output.portName = "outputPortNameOnNode";

    ndi.outputs = new ArrayList<NodeDeployInfo.NodeOutputDeployInfo>();
    ndi.outputs.add(output);
    
    StreamingContainerContext scc = new StreamingContainerContext();
    scc.nodeList = Collections.singletonList(ndi);

    DataOutputByteBuffer out = new DataOutputByteBuffer();
    scc.write(out);

    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(out.getData());

    StreamingContainerContext clone = new StreamingContainerContext();
    clone.readFields(in);

    Assert.assertNotNull(clone.nodeList);
    Assert.assertEquals(1, clone.nodeList.size());
    Assert.assertEquals("node1", clone.nodeList.get(0).declaredId);
    
    String nodeToString = ndi.toString();
    Assert.assertTrue(nodeToString.contains(input.portName));
    Assert.assertTrue(nodeToString.contains(output.portName));
  }
  
  @Test
  public void testAssignContainer() {

    TopologyBuilder b = new TopologyBuilder();

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
    tplg.setMaxContainerCount(2);
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
    Assert.assertEquals("sourcePortName " + c2n1n2, EchoNode.OUTPUT1, c2n1n2.sourcePortName);

    // inline input node3 from node2
    NodeInputDeployInfo c2n3In = getInputDeployInfo(node3DI, "n2n3");
    Assert.assertNotNull("input " + c2n3In, node2DI);
    Assert.assertEquals("portName " + c2n3In, EchoNode.INPUT1, c2n3In.portName);
    Assert.assertNotNull("stream connection for container2", c2n3In);
    Assert.assertNull("bufferServerHost " + c2n3In, c2n3In.bufferServerHost);
    Assert.assertEquals("bufferServerPort " + c2n3In, 0, c2n3In.bufferServerPort);
    Assert.assertNull("partitionKeys " + c2n3In, c2n3In.partitionKeys);
    Assert.assertEquals("sourceNodeId " + c2n3In, node2DI.id, c2n3In.sourceNodeId);
    Assert.assertEquals("sourcePortName " + c2n3In, EchoNode.OUTPUT1, c2n3In.sourcePortName);
  }

  @Test
  public void testStaticPartitioning() {
    TopologyBuilder b = new TopologyBuilder();
    
    NodeConf node1 = b.getOrAddNode("node1");
    NodeConf node2 = b.getOrAddNode("node2");
    NodeConf mergeNode = b.getOrAddNode("mergeNode");
    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }
    
    StreamConf n1n2 = b.getOrAddStream("n1n2")
      .addProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME, TestStaticPartitioningSerDe.class.getName())
      .setSource(EchoNode.OUTPUT1, node1)
      .addSink(EchoNode.INPUT1, node2);
    
    StreamConf mergeStream = b.getOrAddStream("mergeStream")
        .setSource(EchoNode.OUTPUT1, node2)
        .addSink(EchoNode.INPUT1, mergeNode);
    
    Topology tplg = b.getTopology();
    tplg.setMaxContainerCount(5);
    
    DNodeManager dnm = new DNodeManager(tplg);
    Assert.assertEquals("number required containers", 5, dnm.getNumRequiredContainers());

    String container1Id = "container1";
    StreamingContainerContext c1 = dnm.assignContainerForTest(container1Id, InetSocketAddress.createUnresolved(container1Id+"Host", 9001));
    Assert.assertEquals("number nodes assigned to container", 1, c1.nodeList.size());
    Assert.assertTrue(node2.getId() + " assigned to " + container1Id, containsNodeContext(c1, node1));

    for (int i=0; i<TestStaticPartitioningSerDe.partitions.length; i++) {
      String containerId = "container"+(i+1);
      StreamingContainerContext cc = dnm.assignContainerForTest(containerId, InetSocketAddress.createUnresolved(containerId+"Host", 9001));
      Assert.assertEquals("number nodes assigned to container", 1, cc.nodeList.size());
      Assert.assertTrue(node2.getId() + " assigned to " + containerId, containsNodeContext(cc, node2));
  
      // n1n2 in, mergeStream out
      NodeDeployInfo ndi = cc.nodeList.get(0);
      Assert.assertEquals("inputs " + ndi, 1, ndi.inputs.size());
      Assert.assertEquals("outputs " + ndi, 1, ndi.outputs.size());
      
      NodeInputDeployInfo nidi = ndi.inputs.get(0);
      Assert.assertEquals("stream " + nidi, n1n2.getId(), nidi.declaredStreamId);
      Assert.assertTrue("partition for " + containerId, Arrays.equals(TestStaticPartitioningSerDe.partitions[i], nidi.partitionKeys.get(0)));
      Assert.assertEquals("serde " + nidi, TestStaticPartitioningSerDe.class.getName(), nidi.serDeClassName);
    }
    
    // mergeNode container 
    String mergeContainerId = "mergeNodeContainer";
    StreamingContainerContext cmerge = dnm.assignContainerForTest(mergeContainerId, InetSocketAddress.createUnresolved(mergeContainerId+"Host", 9001));
    Assert.assertEquals("number nodes assigned to " + mergeContainerId, 1, cmerge.nodeList.size());

    NodeDeployInfo mergeNodeDI = getNodeDeployInfo(cmerge,  mergeNode);
    Assert.assertNotNull(mergeNode.getId() + " assigned to " + container1Id, mergeNodeDI);
    Assert.assertEquals("inputs " + mergeNodeDI, 3, mergeNodeDI.inputs.size());
    List<String> sourceNodeIds = new ArrayList<String>();
    for (NodeInputDeployInfo nidi : mergeNodeDI.inputs) {
      Assert.assertEquals("streamName " + nidi, mergeStream.getId(), nidi.declaredStreamId);
      Assert.assertEquals("streamName " + nidi, EchoNode.INPUT1, nidi.portName);
      Assert.assertNotNull("sourceNodeId " + nidi, nidi.sourceNodeId);
      sourceNodeIds.add(nidi.sourceNodeId);
    }
    
    for (PTNode node : dnm.getTopologyDeployer().getNodes(tplg.getNode(node2.getId()))) {
      Assert.assertTrue(sourceNodeIds + " contains " + node.id, sourceNodeIds.contains(node.id));
    }
    Assert.assertEquals("outputs " + mergeNodeDI, 0, mergeNodeDI.outputs.size());
  }  

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

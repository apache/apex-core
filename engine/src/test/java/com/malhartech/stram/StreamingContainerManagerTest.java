/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.api.DAG.OperatorWrapper;
import com.malhartech.engine.DefaultStreamCodec;
import com.malhartech.engine.GenericTestModule;
import com.malhartech.engine.Tuple;
import com.malhartech.stram.OperatorDeployInfo.InputDeployInfo;
import com.malhartech.stram.OperatorDeployInfo.OutputDeployInfo;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.util.AttributeMap;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import junit.framework.Assert;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.junit.Test;

public class StreamingContainerManagerTest {

  @Test
  public void testOperatorDeployInfoSerialization() throws Exception {
    OperatorDeployInfo ndi = new OperatorDeployInfo();
    ndi.declaredId = "node1";
    ndi.id ="1";
    ndi.contextAttributes = new AttributeMap.DefaultAttributeMap<OperatorContext>();
    ndi.contextAttributes.attr(OperatorContext.SPIN_MILLIS).set(100);

    OperatorDeployInfo.InputDeployInfo input = new OperatorDeployInfo.InputDeployInfo();
    input.declaredStreamId = "streamToNode";
    input.portName = "inputPortNameOnNode";
    input.sourceNodeId = "sourceNodeId";

    ndi.inputs = new ArrayList<OperatorDeployInfo.InputDeployInfo>();
    ndi.inputs.add(input);

    OperatorDeployInfo.OutputDeployInfo output = new OperatorDeployInfo.OutputDeployInfo();
    output.declaredStreamId = "streamFromNode";
    output.portName = "outputPortNameOnNode";

    ndi.outputs = new ArrayList<OperatorDeployInfo.OutputDeployInfo>();
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
    OperatorDeployInfo ndiClone = clone.nodeList.get(0);
    Assert.assertEquals(ndi.declaredId, ndiClone.declaredId);

    String nodeToString = ndi.toString();
    Assert.assertTrue(nodeToString.contains(input.portName));
    Assert.assertTrue(nodeToString.contains(output.portName));

    Assert.assertEquals("contextAttributes " + ndiClone.contextAttributes, Integer.valueOf(100), ndiClone.contextAttributes.attr(OperatorContext.SPIN_MILLIS).get());

  }

  @Test
  public void testAssignContainer() {

    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);
    GenericTestModule node3 = dag.addOperator("node3", GenericTestModule.class);

    dag.addStream("n1n2", node1.outport1, node2.inport1);

    dag.addStream("n2n3", node2.outport1, node3.inport1)
      .setInline(true);

    dag.setMaxContainerCount(2);

    Assert.assertEquals("number operators", 3, dag.getAllOperators().size());
    Assert.assertEquals("number root operators", 1, dag.getRootOperators().size());

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    Assert.assertEquals("number required containers", 2, dnm.getNumRequiredContainers());

    String container1Id = "container1";
    String container2Id = "container2";

    // node1 needs to be deployed first, regardless in which order they were given
    StreamingContainerContext c1 = dnm.assignContainerForTest(container1Id, InetSocketAddress.createUnresolved(container1Id+"Host", 9001));
    Assert.assertEquals("number operators assigned to c1", 1, c1.nodeList.size());
    OperatorDeployInfo node1DI = getNodeDeployInfo(c1, dag.getOperatorWrapper(node1));
    Assert.assertNotNull(node1.getName() + " assigned to " + container1Id, node1DI);
    Assert.assertEquals("inputs " + node1DI.declaredId, 0, node1DI.inputs.size());
    Assert.assertEquals("outputs " + node1DI.declaredId, 1, node1DI.outputs.size());
    Assert.assertNotNull("serializedNode " + node1DI.declaredId, node1DI.serializedNode);

    OutputDeployInfo c1n1n2 = node1DI.outputs.get(0);
    Assert.assertNotNull("stream connection for container1", c1n1n2);
    Assert.assertEquals("stream connection for container1", "n1n2", c1n1n2.declaredStreamId);
    Assert.assertEquals("stream connects to upstream host", container1Id + "Host", c1n1n2.bufferServerHost);
    Assert.assertEquals("stream connects to upstream port", 9001, c1n1n2.bufferServerPort);
    Assert.assertFalse("stream inline", c1n1n2.isInline());

    StreamingContainerContext c2 = dnm.assignContainerForTest(container2Id, InetSocketAddress.createUnresolved(container2Id+"Host", 9002));
    Assert.assertEquals("number operators assigned to container", 2, c2.nodeList.size());
    OperatorDeployInfo node2DI = getNodeDeployInfo(c2, dag.getOperatorWrapper(node2));
    OperatorDeployInfo node3DI = getNodeDeployInfo(c2, dag.getOperatorWrapper(node3));
    Assert.assertNotNull(node2.getName() + " assigned to " + container2Id, node2DI);
    Assert.assertNotNull(node3.getName() + " assigned to " + container2Id, node3DI);

    // buffer server input node2 from node1
    InputDeployInfo c2n1n2 = getInputDeployInfo(node2DI, "n1n2");
    Assert.assertNotNull("stream connection for container2", c2n1n2);
    Assert.assertEquals("stream connects to upstream host", container1Id + "Host", c2n1n2.bufferServerHost);
    Assert.assertEquals("stream connects to upstream port", 9001, c2n1n2.bufferServerPort);
    Assert.assertEquals("portName " + c2n1n2, dag.getOperatorWrapper(node2).getInputPortMeta(node2.inport1).getPortName(), c2n1n2.portName);
    Assert.assertNull("partitionKeys " + c2n1n2, c2n1n2.partitionKeys);
    Assert.assertEquals("sourceNodeId " + c2n1n2, node1DI.id, c2n1n2.sourceNodeId);
    Assert.assertEquals("sourcePortName " + c2n1n2, GenericTestModule.OPORT1, c2n1n2.sourcePortName);

    // inline input node3 from node2
    InputDeployInfo c2n3In = getInputDeployInfo(node3DI, "n2n3");
    Assert.assertNotNull("input " + c2n3In, node2DI);
    Assert.assertEquals("portName " + c2n3In, GenericTestModule.IPORT1, c2n3In.portName);
    Assert.assertNotNull("stream connection for container2", c2n3In);
    Assert.assertNull("bufferServerHost " + c2n3In, c2n3In.bufferServerHost);
    Assert.assertEquals("bufferServerPort " + c2n3In, 0, c2n3In.bufferServerPort);
    Assert.assertNull("partitionKeys " + c2n3In, c2n3In.partitionKeys);
    Assert.assertEquals("sourceNodeId " + c2n3In, node2DI.id, c2n3In.sourceNodeId);
    Assert.assertEquals("sourcePortName " + c2n3In, GenericTestModule.OPORT1, c2n3In.sourcePortName);
  }

  @Test
  public void testStaticPartitioning() {
    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);
    GenericTestModule mergeNode = dag.addOperator("mergeNode", GenericTestModule.class);

    DAG.StreamDecl n1n2 = dag.addStream("n1n2", node1.outport1, node2.inport1)
      .setSerDeClass(TestStaticPartitioningSerDe.class);

    DAG.StreamDecl mergeStream = dag.addStream("mergeStream", node2.outport1, mergeNode.inport1);

    dag.setMaxContainerCount(5);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    Assert.assertEquals("number required containers", 5, dnm.getNumRequiredContainers());

    String container1Id = "container1";
    StreamingContainerContext c1 = dnm.assignContainerForTest(container1Id, InetSocketAddress.createUnresolved(container1Id+"Host", 9001));
    Assert.assertEquals("number operators assigned to container", 1, c1.nodeList.size());
    Assert.assertTrue(node2.getName() + " assigned to " + container1Id, containsNodeContext(c1, dag.getOperatorWrapper(node1)));

    for (int i=0; i<TestStaticPartitioningSerDe.partitions.length; i++) {
      String containerId = "container"+(i+1);
      StreamingContainerContext cc = dnm.assignContainerForTest(containerId, InetSocketAddress.createUnresolved(containerId+"Host", 9001));
      Assert.assertEquals("number operators assigned to container", 1, cc.nodeList.size());
      Assert.assertTrue(node2.getName() + " assigned to " + containerId, containsNodeContext(cc, dag.getOperatorWrapper(node2)));

      // n1n2 in, mergeStream out
      OperatorDeployInfo ndi = cc.nodeList.get(0);
      Assert.assertEquals("inputs " + ndi, 1, ndi.inputs.size());
      Assert.assertEquals("outputs " + ndi, 1, ndi.outputs.size());

      InputDeployInfo nidi = ndi.inputs.get(0);
      Assert.assertEquals("stream " + nidi, n1n2.getId(), nidi.declaredStreamId);
      Assert.assertTrue("partition for " + containerId, Arrays.equals(TestStaticPartitioningSerDe.partitions[i], nidi.partitionKeys.get(0)));
      Assert.assertEquals("serde " + nidi, TestStaticPartitioningSerDe.class.getName(), nidi.serDeClassName);
    }

    // mergeNode container
    String mergeContainerId = "mergeNodeContainer";
    StreamingContainerContext cmerge = dnm.assignContainerForTest(mergeContainerId, InetSocketAddress.createUnresolved(mergeContainerId+"Host", 9001));
    Assert.assertEquals("number operators assigned to " + mergeContainerId, 1, cmerge.nodeList.size());

    OperatorDeployInfo mergeNodeDI = getNodeDeployInfo(cmerge,  dag.getOperatorWrapper(mergeNode));
    Assert.assertNotNull(mergeNode.getName() + " assigned to " + container1Id, mergeNodeDI);
    Assert.assertEquals("inputs " + mergeNodeDI, 3, mergeNodeDI.inputs.size());
    List<String> sourceNodeIds = new ArrayList<String>();
    for (InputDeployInfo nidi : mergeNodeDI.inputs) {
      Assert.assertEquals("streamName " + nidi, mergeStream.getId(), nidi.declaredStreamId);
      Assert.assertEquals("portName " + nidi, dag.getOperatorWrapper(mergeNode).getInputPortMeta(mergeNode.inport1).getPortName(), nidi.portName);
      Assert.assertNotNull("sourceNodeId " + nidi, nidi.sourceNodeId);
      sourceNodeIds.add(nidi.sourceNodeId);
    }

    for (PTOperator node : dnm.getPhysicalPlan().getOperators(dag.getOperatorWrapper(node2))) {
      Assert.assertTrue(sourceNodeIds + " contains " + node.id, sourceNodeIds.contains(node.id));
    }
    Assert.assertEquals("outputs " + mergeNodeDI, 0, mergeNodeDI.outputs.size());
  }

  /**
   * Verify buffer server address when downstream node is assigned before upstream.
   */
  @Test
  public void testBufferServerAssignment() {
    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);
    GenericTestModule node3 = dag.addOperator("node3", GenericTestModule.class);

    dag.addStream("n1n2", node1.outport1, node2.inport1)
      .setSerDeClass(TestStaticPartitioningSerDe.class);

    dag.addStream("n2n3", node2.outport1, node3.inport1);

    dag.setMaxContainerCount(2);

    // node1 and node3 are assigned, node2 unassigned
    StreamingContainerManager dnmgr = new StreamingContainerManager(dag);
    dnmgr.assignContainerForTest("container1", InetSocketAddress.createUnresolved("localhost", 9001));

  }

  public static class TestStaticPartitioningSerDe extends DefaultStreamCodec {

    public final static byte[][] partitions = new byte[][]{
        {'1'}, {'2'}, {'3'}
    };

    @Override
    public byte[][] getPartitions() {
      return partitions;
    }

    @Override
    public byte[] getPartition(Object o)
    {
      if (o instanceof Tuple) {
        throw new UnsupportedOperationException("should not be called with control tuple");
      }
      return partitions[0];
    }

  }

  private boolean containsNodeContext(StreamingContainerContext scc, OperatorWrapper nodeConf) {
    return getNodeDeployInfo(scc, nodeConf) != null;
  }

  private static OperatorDeployInfo getNodeDeployInfo(StreamingContainerContext scc, OperatorWrapper nodeConf) {
    for (OperatorDeployInfo ndi : scc.nodeList) {
      if (nodeConf.getId().equals(ndi.declaredId)) {
        return ndi;
      }
    }
    return null;
  }

  private static InputDeployInfo getInputDeployInfo(OperatorDeployInfo ndi, String streamId) {
    for (InputDeployInfo in : ndi.inputs) {
      if (streamId.equals(in.declaredStreamId)) {
        return in;
      }
    }
    return null;
  }

}

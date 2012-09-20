/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.google.common.collect.Sets;
import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.GenericTestNode;
import com.malhartech.stram.conf.NewTopologyBuilder;
import com.malhartech.stram.conf.NewTopologyBuilder.StreamBuilder;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.Topology.InputPort;
import com.malhartech.stram.conf.Topology.NodeDecl;
import com.malhartech.stram.conf.Topology.StreamDecl;
import com.malhartech.stram.conf.TopologyBuilder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

public class TopologyBuilderTest {

  public static NodeDecl assertNode(Topology tplg, String id) {
      NodeDecl n = tplg.getNode(id);
      assertNotNull("node exists id=" + id, n);
      return n;
  }

  /**
   * Test read from stram-site.xml in Hadoop configuration format.
   */
  @Test
  public void testLoadFromConfigXml() {
    Configuration conf = TopologyBuilder.addStramResources(new Configuration());
    //Configuration.dumpConfiguration(conf, new PrintWriter(System.out));

    TopologyBuilder tb = new TopologyBuilder(conf);
    Topology tplg = tb.getTopology();
    tplg.validate();

//    Map<String, NodeConf> nodeConfs = tb.getAllNodes();
    assertEquals("number of node confs", 6, tplg.getAllNodes().size());

    NodeDecl node1 = assertNode(tplg, "node1");
    NodeDecl node2 = assertNode(tplg, "node2");
    NodeDecl node3 = assertNode(tplg, "node3");
    NodeDecl node4 = assertNode(tplg, "node4");

    assertNotNull("nodeConf for root", node1);
    assertEquals("nodeId set", "node1", node1.getId());

    // verify node instantiation
    AbstractNode dNode = initNode(node1);
    assertNotNull(dNode);
    assertEquals(dNode.getClass(), GenericTestNode.class);
    GenericTestNode GenericTestNode = (GenericTestNode)dNode;
    assertEquals("myStringPropertyValue", GenericTestNode.getMyStringProperty());

    // check links
    assertEquals("node1 inputs", 0, node1.getInputStreams().size());
    assertEquals("node1 outputs", 1, node1.getOutputStreams().size());
    StreamDecl n1n2 = node2.getInputStreams().get(GenericTestNode.INPUT1);
    assertNotNull("n1n2", n1n2);

    // output/input stream object same
    assertEquals("rootNode out is node2 in", n1n2, node1.getOutputStreams().get(GenericTestNode.OUTPUT1));
    assertEquals("n1n2 source", node1, n1n2.getSource().getNode());
    Assert.assertEquals("n1n2 targets", 1, n1n2.getSinks().size());
    Assert.assertEquals("n1n2 target", node2, n1n2.getSinks().get(0).getNode());

    assertEquals("stream name", "n1n2", n1n2.getId());
    Assert.assertFalse("n1n2 not inline (default)", n1n2.isInline());

    // node 2 streams to node 3 and node 4
    assertEquals("node 2 number of outputs", 1, node2.getOutputStreams().size());
    StreamDecl fromNode2 = node2.getOutputStreams().values().iterator().next();

    Set<NodeDecl> targetNodes = new HashSet<NodeDecl>();
    for (InputPort ip : fromNode2.getSinks()) {
      targetNodes.add(ip.getNode());
    }
    Assert.assertEquals("outputs " + fromNode2, Sets.newHashSet(node3, node4), targetNodes);

    NodeDecl node6 = assertNode(tplg, "node6");

    List<NodeDecl> rootNodes = tplg.getRootNodes();
    assertEquals("number root nodes", 2, rootNodes.size());
    assertTrue("root node2", rootNodes.contains(node1));
    assertTrue("root node6", rootNodes.contains(node6));

    for (NodeDecl n : rootNodes) {
      printTopology(n, tplg, 0);
    }

  }

  @SuppressWarnings("unchecked")
  private <T extends AbstractNode> T initNode(NodeDecl nodeConf) {
    return (T)StramUtils.initNode(nodeConf.getNodeClass(), nodeConf.getId(), nodeConf.getProperties());
  }

  public void printTopology(NodeDecl node, Topology tplg, int level) {
      String prefix = "";
      if (level > 0) {
        prefix = StringUtils.repeat(" ", 20*(level-1)) + "   |" + StringUtils.repeat("-", 17);
      }
      System.out.println(prefix + node.getId());
      for (StreamDecl downStream : node.getOutputStreams().values()) {
          if (!downStream.getSinks().isEmpty()) {
            for (InputPort targetNode : downStream.getSinks()) {
              printTopology(targetNode.getNode(), tplg, level+1);
            }
          }
      }
  }

  @Test
  public void testLoadFromPropertiesFile() throws IOException {
      Properties props = new Properties();
      String resourcePath = "/testTopology.properties";
      InputStream is = this.getClass().getResourceAsStream(resourcePath);
      if (is == null) {
        fail("Could not load " + resourcePath);
      }
      props.load(is);
      TopologyBuilder pb = new TopologyBuilder()
        .addFromProperties(props);

      Topology tplg = pb.getTopology();
      tplg.validate();

      assertEquals("number of node confs", 5, tplg.getAllNodes().size());
      assertEquals("number of root nodes", 1, tplg.getRootNodes().size());

      StreamDecl s1 = tplg.getStream("n1n2");
      assertNotNull(s1);
      assertTrue("n1n2 inline", s1.isInline());

      NodeDecl node3 = tplg.getNode("node3");
      Map<String, String> node3Props = node3.getProperties();

      assertEquals("node3.myStringProperty", "myStringPropertyValueFromTemplate", node3Props.get("myStringProperty"));
      assertEquals("node3.classname", GenericTestNode.class.getName(), node3Props.get(TopologyBuilder.NODE_CLASSNAME));

      GenericTestNode dnode3 = initNode(node3);
      assertEquals("node3.myStringProperty", "myStringPropertyValueFromTemplate", dnode3.getMyStringProperty());
      assertFalse("node3.booleanProperty", dnode3.booleanProperty);

      NodeDecl node4 = tplg.getNode("node4");
      assertEquals("node4.myStringProperty", "overrideNode4", node4.getProperties().get("myStringProperty"));
      GenericTestNode dnode4 = (GenericTestNode)initNode(node4);
      assertEquals("node4.myStringProperty", "overrideNode4", dnode4.getMyStringProperty());
      assertTrue("node4.booleanProperty", dnode4.booleanProperty);

      StreamDecl input1 = tplg.getStream("inputStream");
      assertNotNull(input1);
      Assert.assertEquals("input1 source", tplg.getNode("inputNode"), input1.getSource().getNode());
      Set<NodeDecl> targetNodes = new HashSet<NodeDecl>();
      for (InputPort targetPort : input1.getSinks()) {
        targetNodes.add(targetPort.getNode());
      }

      Assert.assertEquals("input1 target ", Sets.newHashSet(tplg.getNode("node1"), node3, node4), targetNodes);

  }

  @Test
  public void testCycleDetection() {
     NewTopologyBuilder b = new NewTopologyBuilder();

     //NodeConf node1 = b.getOrAddNode("node1");
     NodeDecl node2 = b.addNode("node2", GenericTestNode.class);
     NodeDecl node3 = b.addNode("node3", GenericTestNode.class);
     NodeDecl node4 = b.addNode("node4", GenericTestNode.class);
     //NodeConf node5 = b.getOrAddNode("node5");
     //NodeConf node6 = b.getOrAddNode("node6");
     NodeDecl node7 = b.addNode("node7", GenericTestNode.class);

     // strongly connect n2-n3-n4-n2
     b.addStream("n2n3")
       .setSource(node2.getOutput(GenericTestNode.OUTPUT1))
       .addSink(node3.getInput(GenericTestNode.INPUT1));

     b.addStream("n3n4")
       .setSource(node3.getOutput(GenericTestNode.OUTPUT1))
       .addSink(node4.getInput(GenericTestNode.INPUT1));

     b.addStream("n4n2")
       .setSource(node4.getOutput(GenericTestNode.OUTPUT1))
       .addSink(node2.getInput(GenericTestNode.INPUT1));

     // self referencing node cycle
     StreamBuilder n7n7 = b.addStream("n7n7")
         .setSource(node7.getOutput(GenericTestNode.OUTPUT1))
         .addSink(node7.getInput(GenericTestNode.INPUT1));
     try {
       n7n7.addSink(node7.getInput(GenericTestNode.INPUT1));
       fail("cannot add to stream again");
     } catch (Exception e) {
       // expected, stream can have single input/output only
     }

     Topology tplg = b.getTopology();

     List<List<String>> cycles = new ArrayList<List<String>>();
     tplg.findStronglyConnected(node7, cycles);
     assertEquals("node self reference", 1, cycles.size());
     assertEquals("node self reference", 1, cycles.get(0).size());
     assertEquals("node self reference", node7.getId(), cycles.get(0).get(0));

     // 3 node cycle
     cycles.clear();
     tplg.findStronglyConnected(node4, cycles);
     assertEquals("3 node cycle", 1, cycles.size());
     assertEquals("3 node cycle", 3, cycles.get(0).size());
     assertTrue("node2", cycles.get(0).contains(node2.getId()));
     assertTrue("node3", cycles.get(0).contains(node3.getId()));
     assertTrue("node4", cycles.get(0).contains(node4.getId()));

     try {
       tplg.validate();
       fail("validation should fail");
     } catch (IllegalStateException e) {
       // expected
     }

  }

  public static class TestSerDe extends DefaultSerDe {

  }

  @NodeAnnotation(
      ports = {
          @PortAnnotation(name = "goodOutputPort",  type = PortType.OUTPUT),
          @PortAnnotation(name = "badOutputPort",  type = PortType.OUTPUT)
      }
  )
  static class ValidationNode extends AbstractNode {
    @Override
    public void process(Object payload) {
      // classify tuples
    }
  }

  @NodeAnnotation(
      ports = {
          @PortAnnotation(name = "countInputPort",  type = PortType.INPUT)
      }
  )
  static class CounterNode extends AbstractNode {
    @Override
    public void process(Object payload) {
      // count tuples
    }
  }

  @NodeAnnotation(
      ports = {
          @PortAnnotation(name = "echoInputPort",  type = PortType.INPUT)
      }
  )
  static class ConsoleOutputNode extends AbstractNode {
    @Override
    public void process(Object payload) {
      // print tuples
    }
  }

  @Test
  public void testJavaBuilder() throws Exception {

    NewTopologyBuilder b = new NewTopologyBuilder();

    NodeDecl validationNode = b.addNode("validationNode", ValidationNode.class);
    NodeDecl countGoodNode = b.addNode("countGoodNode", CounterNode.class);
    NodeDecl countBadNode = b.addNode("countBadNode", CounterNode.class);
    NodeDecl echoBadNode = b.addNode("echoBadNode", ConsoleOutputNode.class);

    // good tuples to counter node
    b.addStream("goodTuplesStream")
      .setSource(validationNode.getOutput("goodOutputPort"))
      .addSink(countGoodNode.getInput("countInputPort"));

    // bad tuples to separate stream and echo node
    // (stream with 2 outputs)
    b.addStream("badTuplesStream")
      .setSource(validationNode.getOutput("badOutputPort"))
      .addSink(countBadNode.getInput("countInputPort"))
      .addSink(echoBadNode.getInput("echoInputPort"));

    Topology tplg = b.getTopology();
    Assert.assertEquals("number root nodes", 1, tplg.getRootNodes().size());
    Assert.assertEquals("root node id", "validationNode", tplg.getRootNodes().get(0).getId());

    System.out.println(b.getTopology());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Topology.write(tplg, bos);

    System.out.println("serialized size: " + bos.toByteArray().length);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    Topology tplgClone = Topology.read(bis);
    Assert.assertNotNull(tplgClone);
    Assert.assertEquals("number nodes in clone", tplg.getAllNodes().size(), tplgClone.getAllNodes().size());
    Assert.assertEquals("number root nodes in clone", 1, tplgClone.getRootNodes().size());
    Assert.assertTrue("root node in nodes", tplgClone.getAllNodes().contains(tplgClone.getRootNodes().get(0)));
  }

}

package com.malhartech.stram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeContext;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;

public class TopologyBuilderTest {

  public static NodeConf assertNode(Map<String, NodeConf> nodeConfs, String id) {
      NodeConf n = nodeConfs.get(id);
      assertNotNull("node exists id=" + id, n);
      return n;
  }
  
  @Test
  public void testTopologyRead() {
    Configuration conf = TopologyBuilder.addStramResources(new Configuration());
    //Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
    
    TopologyBuilder topConf = new TopologyBuilder(conf);
    Map<String, NodeConf> nodeConfs = topConf.getAllNodes();
    assertEquals("number of node confs", 6, nodeConfs.size());
    
    NodeConf node1 = assertNode(nodeConfs, "node1");
    NodeConf node2 = assertNode(nodeConfs, "node2");
    NodeConf node3 = assertNode(nodeConfs, "node3");
    NodeConf node4 = assertNode(nodeConfs, "node4");

    assertNotNull("nodeConf for root", node1);
    assertEquals("nodeId set", "node1", node1.getId());

    // verify node instantiation
    AbstractNode dNode = initNode(node1, conf);
    assertNotNull(dNode);
    assertEquals(dNode.getClass(), EchoNode.class);
    EchoNode echoNode = (EchoNode)dNode;
    assertEquals("myStringPropertyValue", echoNode.getMyStringProperty());

    // check links
    assertEquals("node1 inputs", 0, node1.getInputStreams().size());
    assertEquals("node1 outputs", 1, node1.getOutputStreams().size());
    StreamConf n1n2 = node2.getInput("n1n2");
    assertNotNull("n1n2", n1n2);
   
    // output/input stream object same
    assertEquals("rootNode out is node2 in", n1n2, node1.getOutput("n1n2"));
    assertEquals("n1n2 source", node1, n1n2.getSourceNode());
    assertEquals("n1n2 target", node2, n1n2.getTargetNode());
    assertEquals("partitionPolicy", n1n2.getProperty("partitionPolicy"), "someTargetPolicy");
    assertEquals("stream name", "n1n2", n1n2.getId());

    
    // node 2 streams to node 3 and node 4
    assertEquals("node 2 number of outputs", 2, node2.getOutputStreams().size());
    assertNotNull(node2.getOutput("n2n3"));
    assertNotNull(node2.getOutput("n2n4"));

    topConf.validate();

    NodeConf node6 = assertNode(nodeConfs, "node6");
    
    Set<NodeConf> rootNodes = topConf.getRootNodes();
    assertEquals("number root nodes", 2, rootNodes.size());
    assertTrue("root node2", rootNodes.contains(node1));
    assertTrue("root node6", rootNodes.contains(node6));
    
    for (NodeConf n : rootNodes) {
      printTopology(n, nodeConfs, 0);
    }
    
  }

  @SuppressWarnings("unchecked")
  private <T extends AbstractNode> T initNode(NodeConf nodeConf, Configuration conf) {
    StreamingNodeContext snc = DNodeManager.createNodeContext(nodeConf.getId(), nodeConf);
    return (T)StramChild.initNode(snc, conf);
  }
  
  public void printTopology(NodeConf node, Map<String, NodeConf> allNodes, int level) {
      String prefix = "";
      if (level > 0) {
        prefix = StringUtils.repeat(" ", 20*(level-1)) + "   |" + StringUtils.repeat("-", 17); 
      }
      System.out.println(prefix + node.getId());
      for (StreamConf downStream : node.getOutputStreams()) {
          if (downStream.getTargetNode() != null) {
            printTopology(downStream.getTargetNode(), allNodes, level+1);
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
      TopologyBuilder b = new TopologyBuilder(new Configuration());
      b.addFromProperties(props);
      assertEquals("number of node confs", 4, b.getAllNodes().size());
      assertEquals("number of root nodes", 3, b.getRootNodes().size());

      StreamConf s1 = b.getOrAddStream("n1n2");
      assertNotNull(s1);
      assertEquals("n1n2 policy default", "defaultStreamPolicy", s1.getProperty("partitionPolicy"));
      
      NodeConf node3 = b.getOrAddNode("node3");
      Map<String, String> node3Props = node3.getProperties();
      
      assertEquals("node3.myStringProperty", "myStringPropertyValueFromTemplate", node3Props.get("myStringProperty"));
      assertEquals("node3.classname", EchoNode.class.getName(), node3Props.get(TopologyBuilder.NODE_CLASSNAME));

      EchoNode dnode3 = initNode(node3, new Configuration());
      assertEquals("node3.myStringProperty", "myStringPropertyValueFromTemplate", dnode3.myStringProperty);
      
      NodeConf node4 = b.getOrAddNode("node4");
      assertEquals("node4.myStringProperty", "overrideNode4", node4.getProperties().get("myStringProperty"));
      EchoNode dnode4 = (EchoNode)initNode(node4, new Configuration());
      assertEquals("node4.myStringProperty", "overrideNode4", dnode4.myStringProperty);
      
  }
  
  @Test
  public void testCycleDetection() {
     // blank configuration w/o default stram resources
     Configuration conf = new Configuration();    
     TopologyBuilder b = new TopologyBuilder(conf);
     
     //NodeConf node1 = b.getOrAddNode("node1");
     NodeConf node2 = b.getOrAddNode("node2");
     NodeConf node3 = b.getOrAddNode("node3");
     NodeConf node4 = b.getOrAddNode("node4");
     //NodeConf node5 = b.getOrAddNode("node5");
     //NodeConf node6 = b.getOrAddNode("node6");
     NodeConf node7 = b.getOrAddNode("node7");

     // strongly connect n2-n3-n4-n2
     node2.addOutput(b.getOrAddStream("n2n3"));
     node3.addInput(b.getOrAddStream("n2n3"));

     node3.addOutput(b.getOrAddStream("n3n4"));
     node4.addInput(b.getOrAddStream("n3n4"));

     node4.addOutput(b.getOrAddStream("n4n2"));
     node2.addInput(b.getOrAddStream("n4n2"));
     
     // self referencing node cycle
     node7.addInput(b.getOrAddStream("n7n7"));
     try {
       node7.addInput(b.getOrAddStream("n7n7"));
       fail("cannot add to stream again");
     } catch (Exception e) {
       // expected, stream can have single input/output only
     }
     node7.addOutput(b.getOrAddStream("n7n7"));

     List<List<String>> cycles = new ArrayList<List<String>>();
     b.findStronglyConnected(node7, cycles);
     assertEquals("node self reference", 1, cycles.size());
     assertEquals("node self reference", 1, cycles.get(0).size());
     assertEquals("node self reference", node7.getId(), cycles.get(0).get(0));

     // 3 node cycle
     cycles.clear();
     b.findStronglyConnected(node4, cycles);
     assertEquals("3 node cycle", 1, cycles.size());
     assertEquals("3 node cycle", 3, cycles.get(0).size());
     assertTrue("node2", cycles.get(0).contains(node2.getId()));
     assertTrue("node3", cycles.get(0).contains(node3.getId()));
     assertTrue("node4", cycles.get(0).contains(node4.getId()));
     
     try {
       b.validate();
       fail("validation should fail");
     } catch (IllegalStateException e) {
       // expected
     }
     
  }
  
  
  public static class EchoNode extends AbstractNode {

    public EchoNode(NodeContext ctx) {
      super(ctx);
    }

    private String myStringProperty;

    public String getMyStringProperty() {
      return myStringProperty;
    }

    public void setMyStringProperty(String myStringProperty) {
      this.myStringProperty = myStringProperty;
    }

    @Override
    public void process(NodeContext context) {
      // TODO Auto-generated method stub
      
    }

    @Override
    protected boolean shouldShutdown() {
      return true; // cause stram to exit
    }

  }
  
}

package com.malhar.stram.conf;

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import com.malhar.node.DNode;
import com.malhar.stram.conf.TopologyBuilder.NodeConf;
import com.malhar.stram.conf.TopologyBuilder.StreamConf;

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
    
    NodeConf rootNode = assertNode(nodeConfs, "node1");
    NodeConf node2 = assertNode(nodeConfs, "node2");
    NodeConf node3 = assertNode(nodeConfs, "node3");
    NodeConf node4 = assertNode(nodeConfs, "node4");

    assertNotNull("nodeConf for root", rootNode);
    assertEquals("nodeId set", "node1", rootNode.id);
    
    // verify node instantiation
    DNode dNode = initNode(rootNode, conf);
    assertNotNull(dNode);
    assertEquals(dNode.getClass(), EchoNode.class);
    EchoNode echoNode = (EchoNode)dNode;
    assertEquals("myStringPropertyValue", echoNode.getMyStringProperty());

    // check links
    assertEquals("rootNode inputs", rootNode.inputs.size(), 0);
    assertEquals("rootNode outputs", rootNode.outputs.size(), 1);
    StreamConf rootNodeOut = rootNode.outputs.get(node2);
    StreamConf node2In = node2.inputs.get(rootNode);
   
    // output/input stream object same
    assertEquals("rootNode out is node2 in", rootNodeOut, node2In);
    assertEquals("partitionPolicy", node2In.getProperty("partitionPolicy"), "someTargetPolicy");
    assertEquals("stream name", "streamNameNode1Node2", node2In.getName());

    
    // node 2 streams to node 3 and node 4
    assertEquals("node 2 number of outputs", 2, node2.outputs.size());
    assertNotNull(node2.outputs.get(node3));
    assertNotNull(node2.outputs.get(node4));

    topConf.validate();

    NodeConf node6 = assertNode(nodeConfs, "node6");
    
    Set<NodeConf> rootNodes = topConf.getRootNodes();
    assertEquals("number root nodes", 2, rootNodes.size());
    assertTrue("root node2", rootNodes.contains(rootNode));
    assertTrue("root node6", rootNodes.contains(node6));
    
    for (NodeConf n : rootNodes) {
      printTopology(n, nodeConfs, 0);
    }
    
  }

  public void printTopology(NodeConf node, Map<String, NodeConf> allNodes, int level) {
      String prefix = "";
      if (level > 0) {
        prefix = StringUtils.repeat(" ", 20*(level-1)) + "   |" + StringUtils.repeat("-", 17); 
      }
      System.out.println(prefix + node.id);
      for (NodeConf downStreamNode : node.outputs.keySet()) {
          printTopology(downStreamNode, allNodes, level+1);
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
      assertEquals("number of node confs", 2, b.getAllNodes().size());
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
     //odeConf node5 = b.getOrAddNode("node5");
     //NodeConf node6 = b.getOrAddNode("node6");
     NodeConf node7 = b.getOrAddNode("node7");

     // strongly connect n2-n3-n4-n2
     node3.addInput(node2);
     node4.addInput(node3);
     node2.addInput(node4);

     // self referencing node cycle
     node7.addInput(node7);

     List<List<String>> cycles = new ArrayList<List<String>>();
     b.findStronglyConnected(node7, cycles);
     assertEquals("node self reference", 1, cycles.size());
     assertEquals("node self reference", 1, cycles.get(0).size());
     assertEquals("node self reference", node7.id, cycles.get(0).get(0));

     // 3 node cycle
     cycles.clear();
     b.findStronglyConnected(node4, cycles);
     assertEquals("node self reference", 1, cycles.size());
     assertEquals("node self reference", 3, cycles.get(0).size());
     assertTrue("node2", cycles.get(0).contains(node2.id));
     assertTrue("node3", cycles.get(0).contains(node3.id));
     assertTrue("node4", cycles.get(0).contains(node4.id));
     
     try {
       b.validate();
       fail("validation should fail");
     } catch (IllegalStateException e) {
       // expected
     }
     
  }
  
  /**
   * TODO: Move to Stram initialization
   * Instantiate node from configuration. 
   * (happens in the execution container, not the stram master process.)
   * @param nodeConf
   * @param conf
   */
  public static DNode initNode(NodeConf nodeConf, Configuration conf) {
    String className = nodeConf.properties.get(TopologyBuilder.NODE_CLASSNAME);
    if (className == null) {
      throw new IllegalArgumentException(String.format("Configuration for node '%s' is missing property '%s'", nodeConf.id, TopologyBuilder.NODE_CLASSNAME));
    }
    try {
      Class<? extends DNode> nodeClass = Class.forName(className).asSubclass(DNode.class);    
      DNode node = ReflectionUtils.newInstance(nodeClass, conf);
      // populate the custom properties
      BeanUtils.populate(node, nodeConf.properties);
      return node;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Node class not found: " + className, e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
  }
  
  public static class EchoNode extends DNode {

    private String myStringProperty;

    public String getMyStringProperty() {
      return myStringProperty;
    }

    public void setMyStringProperty(String myStringProperty) {
      this.myStringProperty = myStringProperty;
    }
    
  }
  
}

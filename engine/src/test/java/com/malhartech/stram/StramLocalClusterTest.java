/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stream.HDFSOutputStream;

/**
 *
 */
public class StramLocalClusterTest {

  @Test
  public void testLocalClusterInitShutdown() {
    // create test topology
    Properties props = new Properties();

    // input adapter to ensure shutdown works while windows are generated
//    props.put("stram.stream.input1.classname", NumberGeneratorInputAdapter.class.getName());
//    props.put("stram.stream.input1.outputNode", "node1");
//    props.put("stram.stream.input1.maxTuples", "1");

    // fake output adapter - to be ignored when determine shutdown
    props.put("stram.stream.output.classname", HDFSOutputStream.class.getName());
    props.put("stram.stream.output.inputNode", "node2");
    props.put("stram.stream.output.filepath", "miniclustertest-testSetupShutdown.out");

    props.put("stram.stream.n1n2.inputNode", "node1");
    props.put("stram.stream.n1n2.outputNode", "node2");
    props.put("stram.stream.n1n2.template", "defaultstream");

    props.put("stram.node.node1.classname", TopologyBuilderTest.EchoNode.class.getName());
    props.put("stram.node.node1.myStringProperty", "myStringPropertyValue");

    props.put("stram.node.node2.classname", TopologyBuilderTest.EchoNode.class.getName());

    props.setProperty(TopologyBuilder.NUM_CONTAINERS, "2");

    TopologyBuilder tplg = new TopologyBuilder(new Configuration());
    tplg.addFromProperties(props);
    
    StramLocalCluster localCluster = new StramLocalCluster(tplg);
    localCluster.run();
    
    
  }
  
}

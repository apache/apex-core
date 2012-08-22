/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import static com.malhartech.stram.conf.TopologyBuilder.STRAM_WINDOW_SIZE_MILLIS;
import static com.malhartech.stram.conf.TopologyBuilder.STREAM_CLASSNAME;
import static com.malhartech.stram.conf.TopologyBuilder.STREAM_INLINE;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.malhartech.dag.InputAdapter;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;
import com.malhartech.stream.HDFSOutputStream;

public class StramLocalClusterTest {

  @Test
  public void testLocalClusterInitShutdown() throws Exception {
    // create test topology
    Properties props = new Properties();

    // input adapter to ensure shutdown works on end of stream
    props.put("stram.stream.input1.classname", NumberGeneratorInputAdapter.class.getName());
    props.put("stram.stream.input1.outputNode", "node1");
    props.put("stram.stream.input1.maxTuples", "1");

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

  //@Test
  public void testChildRecovery() throws Exception {

    TopologyBuilder tb = new TopologyBuilder();
    tb.getConf().setInt(STRAM_WINDOW_SIZE_MILLIS, 0); // disable window generator

    StreamConf input1 = tb.getOrAddStream("input1");
    input1.addProperty(STREAM_CLASSNAME,
                       NumberGeneratorInputAdapter.class.getName());
    input1.addProperty(STREAM_INLINE, "true");
    input1.addProperty("maxTuples", "1");

    NodeConf node1 = tb.getOrAddNode("node1");
    node1.addInput(input1);

    for (NodeConf nodeConf : tb.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    StramLocalCluster localCluster = new StramLocalCluster(tb);
    localCluster.runAsync();

    List<InputAdapter> inputAdapters = localCluster.getContainer(0).getInputAdapters();
    Assert.assertEquals("number input adapters", 1, inputAdapters.size());

    InputAdapter input = inputAdapters.get(0);
    input.resetWindow(0, 1);
    input.beginWindow(1);


  }


}

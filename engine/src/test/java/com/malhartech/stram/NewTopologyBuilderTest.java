/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import junit.framework.Assert;

import org.junit.Test;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;
import com.malhartech.stram.conf.NewTopologyBuilder;
import com.malhartech.stram.conf.NewTopologyBuilder.NodeBuilder;
import com.malhartech.stram.conf.Topology;

/**
 *
 */
public class NewTopologyBuilderTest {

  @NodeAnnotation(
      ports = {
          @PortAnnotation(name = "goodOutputPort",  type = PortType.OUTPUT),
          @PortAnnotation(name = "badOutputPort",  type = PortType.OUTPUT)
      }
  )
  static class ValidationNode extends AbstractNode implements Serializable {
    private static final long serialVersionUID = 1L;

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
  static class CounterNode extends AbstractNode implements Serializable {
    private static final long serialVersionUID = 1L;

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
  static class ConsoleOutputNode extends AbstractNode implements Serializable {
    private static final long serialVersionUID = 1L;
    
    @Override
    public void process(Object payload) {
      // print tuples
    }
  }
  
  @Test
  public void testBuilder() throws Exception {

    NewTopologyBuilder b = new NewTopologyBuilder();
    
    NodeBuilder validationNode = b.addNode("validationNode", new ValidationNode());
    NodeBuilder countGoodNode = b.addNode("countGoodNode", new CounterNode());
    NodeBuilder countBadNode = b.addNode("countBadNode", new CounterNode());
    NodeBuilder echoBadNode = b.addNode("echoBadNode", new ConsoleOutputNode());

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
    
    System.out.println(b.getTopology().getRootNodes());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(tplg);
  
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    Topology tplgClone = (Topology)new ObjectInputStream(bis).readObject();
    Assert.assertNotNull(tplgClone);
    Assert.assertEquals("number nodes in clode", 1, tplgClone.getRootNodes().size());
    
  }
  
  
}

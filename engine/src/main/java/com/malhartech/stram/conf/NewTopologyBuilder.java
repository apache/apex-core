/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.conf;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;


public class NewTopologyBuilder {

  class InputPort {
  }

  class OutputPort {
  }

  class StreamDeclaration {
    StreamDeclaration addSink(InputPort port) {
      return this;
    }
    
    StreamDeclaration setSource(OutputPort port) {
      return this;
    }
  }
  
  class NodeDeclaration {
    NodeDeclaration(String id, AbstractNode node) {
      
    }

    InputPort getInput(String portName) {
      throw new UnsupportedOperationException();
    }
    
    OutputPort getOutput(String portName) {
      throw new UnsupportedOperationException();
    }
    
  }

  NodeDeclaration addNode(String id, AbstractNode node) {
     return new NodeDeclaration(id, node);
    
  }
  
  StreamDeclaration addStream(String id) {
    throw new UnsupportedOperationException();
  }

  
  public static void main(String[] args) {

    @NodeAnnotation(
        ports = {
            @PortAnnotation(name = "goodOutputPort",  type = PortType.OUTPUT),
            @PortAnnotation(name = "badOutputPort",  type = PortType.OUTPUT)
        }
    )
    class ValidationNode extends AbstractNode {
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
    class CounterNode extends AbstractNode {
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
    class ConsoleOutputNode extends AbstractNode {
      @Override
      public void process(Object payload) {
        // print tuples
      }
    }
    
    
    NewTopologyBuilder b = new NewTopologyBuilder();
    
    NodeDeclaration validationNode = b.addNode("validationNode", new ValidationNode());
    NodeDeclaration countGoodNode = b.addNode("countGoodNode", new CounterNode());
    NodeDeclaration countBadNode = b.addNode("countBadNode", new CounterNode());
    NodeDeclaration echoBadNode = b.addNode("echoBadNode", new ConsoleOutputNode());

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
  }
  
} 
  

/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.conf;

import com.malhartech.dag.AbstractNode;
import com.malhartech.stram.conf.Topology.InputPort;
import com.malhartech.stram.conf.Topology.NodeDecl;
import com.malhartech.stram.conf.Topology.OutputPort;
import com.malhartech.stram.conf.Topology.StreamDecl;


public class NewTopologyBuilder {

  private Topology topology = new Topology();;
  
  public class StreamBuilder {
    final private StreamDecl streamDecl;

    private StreamBuilder(StreamDecl streamDecl) {
      this.streamDecl = streamDecl;
    }
    
    public StreamBuilder setInline(boolean inline) {
      streamDecl.setInline(inline);
      return this;
    }
    
    public StreamBuilder addSink(InputPort port) {
      streamDecl.addSink(port);
      return this;
    }

    public StreamBuilder setSource(OutputPort port) {
      streamDecl.setSource(port);
      return this;
    }
    
  }
  
  public class NodeBuilder {
    final private NodeDecl decl;
    
    private NodeBuilder(NodeDecl decl) {
      this.decl = decl;
    }

    public InputPort getInput(String portName) {
      return decl.getInput(portName);
    }
    
    public OutputPort getOutput(String portName) {
      return decl.getOutput(portName);
    }
  }

  public NodeBuilder addNode(String id, AbstractNode node) {
    return new NodeBuilder(topology.addNode(id, node));
  }
  
  public StreamBuilder addStream(String id) {
    return new StreamBuilder(topology.addStream(id));
  }
  
  public Topology getTopology() {
    return topology;
  }
  
} 

/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.conf;

import com.malhartech.dag.Module;
import com.malhartech.dag.SerDe;
import com.malhartech.stram.conf.DAG.InputPort;
import com.malhartech.stram.conf.DAG.Operator;
import com.malhartech.stram.conf.DAG.OutputPort;
import com.malhartech.stram.conf.DAG.StreamDecl;


public class NewDAGBuilder implements ApplicationFactory {

  private final DAG topology = new DAG();;

  public class StreamBuilder {
    private final StreamDecl streamDecl;

    private StreamBuilder(StreamDecl streamDecl) {
      this.streamDecl = streamDecl;
    }

    public StreamBuilder setInline(boolean inline) {
      streamDecl.setInline(inline);
      return this;
    }

    public StreamBuilder setSerDeClass(Class<? extends SerDe> serDeClass) {
      streamDecl.setSerDeClass(serDeClass);
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

    public StreamDecl getDecl() {
      return streamDecl;
    }

  }

  public Operator addNode(String id, Class<? extends Module> nodeClass) {
    return topology.addNode(id, nodeClass);
  }

  public StreamBuilder addStream(String id) {
    return new StreamBuilder(topology.addStream(id));
  }

  public DAG getTopology() {
    return topology;
  }

  @Override
  public DAG getApplication() {
    return topology;
  }

}

/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.InputAdapter;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.dag.Tuple;
import com.malhartech.stram.conf.TopologyBuilder;

/**
 * Wrapper node to connects adapter "stream" to another source or sink
 * Provides uniform view of adapter as node for stram deployment and monitoring.
 */
public class AdapterWrapperNode extends AbstractNode implements Sink {

  public static final String KEY_STREAM_CLASS_NAME = "streamClassName";
  public static final String KEY_IS_INPUT = "isInput";

  private String streamClassName;
  private boolean isInput;
  private Stream adapterStream = null;
  
  
  public AdapterWrapperNode(NodeContext ctx) {
    super(ctx);
  }

  public String getStreamClassName() {
    return streamClassName;
  }

  public void setStreamClassName(String streamClassName) {
    this.streamClassName = streamClassName;
  }

  public boolean isInput() {
    return isInput;
  }

  public void setInput(boolean isInput) {
    this.isInput = isInput;
  }

  public InputAdapter getInputAdapter() {
    return (InputAdapter)adapterStream;
  }
  
  @Override
  public void process(NodeContext context, StreamContext streamContext,
      Object payload) {
    throw new UnsupportedOperationException("Adapter nodes do not implement process.");
  }

  @Override
  public void doSomething(Tuple t) {
    // pass on the tuple downstream
    for (Sink sink : sinks) {
      sink.doSomething(t);
    }
  }

  @Override
  public void setup(NodeConfiguration config) {
    Map<String, String> props = config.getDagProperties();
    props.put(TopologyBuilder.STREAM_CLASSNAME, this.streamClassName);
    StreamConfiguration streamConf = new StreamConfiguration(props);
    if (isInput) {
      InputAdapter inputAdapter = StramChild.initAdapterStream(streamConf, this);
      adapterStream = inputAdapter;
    } else {
      adapterStream = StramChild.initAdapterStream(streamConf, null);
    }
  }

  @Override
  public void teardown() {
    if (adapterStream != null) {
      adapterStream.teardown();
    }
  }

  private List<Sink> sinks = new ArrayList<Sink>();
  
  @Override
  public void addSink(Sink sink) {
    sinks.add(sink);
  }

  @Override
  public Sink getSink(StreamContext context) {
    // called for output adapter, hand back the stream
    return super.getSink(context);
  }
  
}

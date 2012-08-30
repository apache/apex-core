/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.conf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.Node;
import com.malhartech.dag.SerDe;

/**
 * Topology contains the logical declarations of nodes and streams.
 * It will be serialized and deployed to the cluster, where it is translated into the physical plan.
 */
public class Topology implements Serializable, TopologyConstants {

  private static final long serialVersionUID = -807535341555334841L;
  
  private final Map<String, StreamDecl> streams = new HashMap<String, StreamDecl>();
  private final Map<String, NodeDecl> nodes = new HashMap<String, NodeDecl>();
  private final List<NodeDecl> rootNodes = new ArrayList<NodeDecl>();
  private final Configuration conf;
  
  public Topology() {
    this.conf = new Configuration(false); 
  }
  
  public Topology(Configuration conf) {
    this.conf = conf; 
  }
  
  final public class InputPort implements Serializable {
    private static final long serialVersionUID = 1L;

    private NodeDecl node;
    private PortAnnotation portAnnotation;

    public NodeDecl getNode() {
      return node;
    }
    
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("node", this.node).
          append("port", this.portAnnotation).
          toString();
    }
  }

  final public class OutputPort implements Serializable {
    private static final long serialVersionUID = 1L;

    private NodeDecl node;
    private PortAnnotation portAnnotation;

    public NodeDecl getNode() {
      return node;
    }

    public String getPortName() {
      return portAnnotation.name();
    }
    
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("node", this.node).
          append("port", this.portAnnotation).
          toString();
    }
  }

  final public class StreamDecl implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean inline;
    private List<InputPort> sinks = new ArrayList<InputPort>();
    private OutputPort source; 
    private Class<? extends SerDe> serDeClass;
    private final String id;
    
    private StreamDecl(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }
    
    /**
     * Hint to manager that adjacent nodes should be deployed in same container.
     * @return boolean
     */
    public boolean isInline() {
      return inline;
    }

    public void setInline(boolean inline) {
      this.inline = inline;
    }

    public Class<? extends SerDe> getSerDeClass() {
      return serDeClass;
    }

    public void setSerDeClass(Class<? extends SerDe> serDeClass) {
      this.serDeClass = serDeClass;
    }
    
    public OutputPort getSource() {
      return source;
    }

    public void setSource(OutputPort port) {
      this.source = port;
      port.node.outputStreams.put(port.portAnnotation.name(), this);
    }
    
    public List<InputPort> getSinks() {
      return sinks;
    }

    public void addSink(InputPort port) {
      sinks.add(port);
      port.node.inputStreams.put(port.portAnnotation.name(), this);
      rootNodes.remove(port.node);
    }

  }

  final public class NodeDecl implements Serializable {
    private static final long serialVersionUID = 1L;

    final Map<String, StreamDecl> inputStreams = new HashMap<String, StreamDecl>();
    final Map<String, StreamDecl> outputStreams = new HashMap<String, StreamDecl>();
    final Map<String, String> properties = new HashMap<String, String>();
    final Node node;
    final String id;
    
    private NodeDecl(String id, Node node) {
      this.node = node;
      this.id = id;
    }

    public String getId() {
      return id;
    }
    
    public InputPort getInput(String portName) {
      PortAnnotation pa = findPortAnnotationByName(portName, PortType.INPUT);
      InputPort port = new InputPort();
      port.node = this;
      port.portAnnotation = pa;
      return port;
    }
    
    public OutputPort getOutput(String portName) {
      PortAnnotation pa = findPortAnnotationByName(portName, PortType.OUTPUT);
      OutputPort port = new OutputPort();
      port.node = this;
      port.portAnnotation = pa;
      return port;
    }

    public Map<String, StreamDecl> getInputStreams() {
      return this.inputStreams;
    }

    public Map<String, StreamDecl> getOutputStreams() {
      return this.outputStreams;
    }

    public Node getNode() {
      return this.node;
    }
    
    /**
     * Properties for the node.
     * @return Map<String, String>
     */
    public Map<String, String> getProperties() {
      return properties;
    }
    
    private PortAnnotation findPortAnnotationByName(String portName, PortType type) {
      Class<?> clazz = this.node.getClass();
      NodeAnnotation na = clazz.getAnnotation(NodeAnnotation.class);
      if (na != null) {
        PortAnnotation[] ports = na.ports();
        for (PortAnnotation pa : ports) {
          if (pa.name().equals(portName) && pa.type() == type) {
            return pa;
          }
        }
      }
      String msg = String.format("No port with name %s and type %s found on %s", portName, type, node);
      throw new IllegalArgumentException(msg);
    }
 
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", this.id).
          toString();
    }
    
  }

  
  NodeDecl addNode(String id, Node node) {
    if (nodes.containsKey(id)) {
      throw new IllegalArgumentException("duplicate node id: " + nodes.get(id));
    }
    
    NodeDecl decl = new NodeDecl(id, node);
    rootNodes.add(decl);
    nodes.put(id, decl);
    
    return decl;
  }
  
  
  StreamDecl addStream(String id) {
    StreamDecl s = this.streams.get(id);
    if (s == null) {
      s = new StreamDecl(id);
      this.streams.put(id, s);
    }
    return s;
  }
  
  public List<NodeDecl> getRootNodes() {
     return Collections.unmodifiableList(this.rootNodes);
  }
  
  public Collection<NodeDecl> getAllNodes() {
    return Collections.unmodifiableCollection(this.nodes.values());
  }

  public NodeDecl getNode(String nodeId) {
    return this.nodes.get(nodeId);
  }
  
  public Configuration getConf() {
    return this.conf;
  }
  
  public int getMaxContainerCount() {
    return this.conf.getInt(STRAM_MAX_CONTAINERS, 3);
  }

  public void setMaxContainerCount(int containerCount) {
    this.conf.setInt(STRAM_MAX_CONTAINERS, containerCount);
  }

  public String getLibJars() {
    return conf.get(STRAM_LIBJARS, "");
  }

  public boolean isDebug() {
    return conf.getBoolean(STRAM_DEBUG, false);
  }

  public int getContainerMemoryMB() {
    return conf.getInt(STRAM_CONTAINER_MEMORY_MB, 64);
  }
  
  public int getMasterMemoryMB() {
    return conf.getInt(STRAM_MASTER_MEMORY_MB, 256);
  }

  /**
   * Class dependencies for the topology. Used to determine jar file dependencies. 
   * @return Set<String>
   */
  public Set<String> getClassNames() {
    Set<String> classNames = new HashSet<String>();
    for (NodeDecl n : this.nodes.values()) {
      String className = n.node.getClass().getName();
      if (className != null) {
        classNames.add(className);
      }
    }
    for (StreamDecl n : this.streams.values()) {
      if (n.serDeClass != null) {
        classNames.add(n.serDeClass.getName());
      }
    }
    return classNames;
  }
  
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
        append("nodes", this.nodes).
        append("streams", this.streams).
        append("properties", this.conf).
        toString();
  }
  
}

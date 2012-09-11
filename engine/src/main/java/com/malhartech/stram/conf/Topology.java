/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.conf;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final long serialVersionUID = -2099729915606048704L;

  private static final Logger LOG = LoggerFactory.getLogger(Topology.class);

  private final Map<String, StreamDecl> streams = new HashMap<String, StreamDecl>();
  private final Map<String, NodeDecl> nodes = new HashMap<String, NodeDecl>();
  private final List<NodeDecl> rootNodes = new ArrayList<NodeDecl>();
  private final ExternalizableConf confHolder;

  private transient int nodeIndex = 0; // used for cycle validation
  private transient Stack<NodeDecl> stack = new Stack<NodeDecl>(); // used for cycle validation

  public static class ExternalizableConf implements Externalizable {
    private final Configuration conf;

    public ExternalizableConf(Configuration conf) {
      this.conf = conf;
    }

    public ExternalizableConf() {
      this.conf = new Configuration(false);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      conf.readFields(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      conf.write(out);
    }
  }

  public Topology() {
    this.confHolder = new ExternalizableConf(new Configuration(false));
  }

  public Topology(Configuration conf) {
    this.confHolder = new ExternalizableConf(conf);
  }

  public final class InputPort implements Serializable {
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

  public final class OutputPort implements Serializable {
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

  public final class StreamDecl implements Serializable {
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
      String portName = port.portAnnotation.name();
      if (port.node.inputStreams.containsKey(portName)) {
        throw new IllegalArgumentException(String.format("Port %s already connected to stream %s", portName, port.node.inputStreams.get(portName)));
      }
      sinks.add(port);
      port.node.inputStreams.put(port.portAnnotation.name(), this);
      rootNodes.remove(port.node);
    }

  }

  public final class NodeDecl implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, StreamDecl> inputStreams = new HashMap<String, StreamDecl>();
    private final Map<String, StreamDecl> outputStreams = new HashMap<String, StreamDecl>();
    private final Map<String, String> properties = new HashMap<String, String>();
    private final Class<? extends Node> nodeClass;
    private final String id;

    private transient Integer nindex; // for cycle detection
    private transient Integer lowlink; // for cycle detection

    private NodeDecl(String id, Class<? extends Node> nodeClass) {
      this.nodeClass = nodeClass;
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

    public Class<? extends Node> getNodeClass() {
      return this.nodeClass;
    }

    /**
     * Properties for the node.
     * @return Map<String, String>
     */
    public Map<String, String> getProperties() {
      return properties;
    }

    public NodeDecl setProperty(String name, String value) {
      properties.put(name, value);
      return this;
    }

    private PortAnnotation findPortAnnotationByName(String portName, PortType type) {
      Class<?> clazz = this.nodeClass;
      NodeAnnotation na = clazz.getAnnotation(NodeAnnotation.class);
      if (na != null) {
        PortAnnotation[] ports = na.ports();
        for (PortAnnotation pa : ports) {
          if (pa.name().equals(portName) && pa.type() == type) {
            return pa;
          }
        }
      }
      String msg = String.format("No port with name %s and type %s found for %s (%s)", portName, type, id, nodeClass);
      throw new IllegalArgumentException(msg);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", this.id).
          append("class", this.nodeClass.getName()).
          toString();
    }

  }

  NodeDecl addNode(String id, Class<? extends Node> nodeClass) {
    if (nodes.containsKey(id)) {
      throw new IllegalArgumentException("duplicate node id: " + nodes.get(id));
    }

    NodeDecl decl = new NodeDecl(id, nodeClass);
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

  public StreamDecl getStream(String id) {
    return this.streams.get(id);
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
    return this.confHolder.conf;
  }

  public int getMaxContainerCount() {
    return this.confHolder.conf.getInt(STRAM_MAX_CONTAINERS, 3);
  }

  public void setMaxContainerCount(int containerCount) {
    this.confHolder.conf.setInt(STRAM_MAX_CONTAINERS, containerCount);
  }

  public String getLibJars() {
    return confHolder.conf.get(STRAM_LIBJARS, "");
  }

  public boolean isDebug() {
    return confHolder.conf.getBoolean(STRAM_DEBUG, false);
  }

  public int getContainerMemoryMB() {
    return confHolder.conf.getInt(STRAM_CONTAINER_MEMORY_MB, 64);
  }

  public int getMasterMemoryMB() {
    return confHolder.conf.getInt(STRAM_MASTER_MEMORY_MB, 256);
  }

  /**
   * Class dependencies for the topology. Used to determine jar file dependencies.
   * @return Set<String>
   */
  public Set<String> getClassNames() {
    Set<String> classNames = new HashSet<String>();
    for (NodeDecl n : this.nodes.values()) {
      String className = n.nodeClass.getName();
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

  /**
   * Validate the topology. Includes checks that required ports are connected,
   * required configuration parameters specified, graph free of cycles etc.
   */
  public void validate() {
    // clear visited on all nodes
    for (NodeDecl n : nodes.values()) {
      n.nindex = null;
      n.lowlink = null;
    }

    List<List<String>> cycles = new ArrayList<List<String>>();
    for (NodeDecl n : nodes.values()) {
      if (n.nindex == null) {
        findStronglyConnected(n, cycles);
      }
    }
    if (!cycles.isEmpty()) {
      throw new IllegalStateException("Loops detected in the graph: " + cycles);
    }

    for (StreamDecl s : streams.values()) {
      if (s.source == null && (s.sinks.isEmpty())) {
        throw new IllegalStateException(String.format("stream needs to be connected to at least on node %s", s.getId()));
      }
    }
  }

  /**
   * Check for cycles in the graph reachable from start node n. This is done by
   * attempting to find a strongly connected components, see
   * http://en.wikipedia.
   * org/wiki/Tarjan%E2%80%99s_strongly_connected_components_algorithm
   *
   * @param n
   * @param cycles
   */
  public void findStronglyConnected(NodeDecl n, List<List<String>> cycles) {
    n.nindex = nodeIndex;
    n.lowlink = nodeIndex;
    nodeIndex++;
    stack.push(n);

    // depth first successors traversal
    for (StreamDecl downStream : n.outputStreams.values()) {
      for (InputPort sink : downStream.sinks) {
        NodeDecl successor = sink.node;
        if (successor == null) {
          continue;
        }
        // check for self referencing node
        if (n == successor) {
          cycles.add(Collections.singletonList(n.id));
        }
        if (successor.nindex == null) {
          // not visited yet
          findStronglyConnected(successor, cycles);
          n.lowlink = Math.min(n.lowlink, successor.lowlink);
        } else if (stack.contains(successor)) {
          n.lowlink = Math.min(n.lowlink, successor.nindex);
        }
      }
    }

    // pop stack for all root nodes
    if (n.lowlink.equals(n.nindex)) {
      List<String> connectedIds = new ArrayList<String>();
      while (!stack.isEmpty()) {
        NodeDecl n2 = stack.pop();
        connectedIds.add(n2.id);
        if (n2 == n) {
          break; // collected all connected nodes
        }
      }
      // strongly connected (cycle) if more than one node in stack
      if (connectedIds.size() > 1) {
        LOG.debug("detected cycle from node {}: {}", n.id, connectedIds);
        cycles.add(connectedIds);
      }
    }
  }

  public static void write(Topology tplg, OutputStream os) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(os);
    oos.writeObject(tplg);
  }

  public static Topology read(InputStream is) throws IOException, ClassNotFoundException {
    return (Topology)new ObjectInputStream(is).readObject();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
        append("nodes", this.nodes).
        append("streams", this.streams).
        append("properties", TopologyBuilder.toProperties(this.confHolder.conf)).
        toString();
  }

}

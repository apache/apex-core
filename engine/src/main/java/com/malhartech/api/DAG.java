/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
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

import com.malhartech.dag.DAGConstants;
import com.malhartech.dag.DefaultModuleSerDe;
import com.malhartech.dag.SerDe;
import com.malhartech.moduleexperiment.ProtoInputPortFieldAnnotation;
import com.malhartech.moduleexperiment.ProtoOutputPortFieldAnnotation;
import com.malhartech.stram.DAGPropertiesBuilder;

/**
 * DAG contains the logical declarations of operators and streams.
 * It will be serialized and deployed to the cluster, where it is translated into the physical plan.
 */
public class DAG implements Serializable, DAGConstants {
  private static final long serialVersionUID = -2099729915606048704L;

  private static final Logger LOG = LoggerFactory.getLogger(DAG.class);

  private final Map<String, StreamDecl> streams = new HashMap<String, StreamDecl>();
  private final Map<String, OperatorWrapper> nodes = new HashMap<String, OperatorWrapper>();
  private final List<OperatorWrapper> rootNodes = new ArrayList<OperatorWrapper>();

  private final ExternalizableConf confHolder;

  private transient int nodeIndex = 0; // used for cycle validation
  private transient Stack<OperatorWrapper> stack; // used for cycle validation

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

  public static class ExternalizableModule implements Externalizable {
    private Operator module;
    // since the ports are transient, we cannot serialize them
    private final Map<Operator.InputPort<?>, InputPortMeta> inPortMap = new HashMap<Operator.InputPort<?>, InputPortMeta>();
    private final Map<Operator.OutputPort<?>, OutputPortMeta> outPortMap = new HashMap<Operator.OutputPort<?>, OutputPortMeta>();

    private void set(Operator module) {
      this.module = module;
      mapInputPorts(this, inPortMap);
      mapOutputPorts(this,  outPortMap);
    }

    private Operator get() {
      return this.module;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.read(bytes);
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      set((Operator)new DefaultModuleSerDe().read(bis));
      bis.close();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      new DefaultModuleSerDe().write(module, bos);
      bos.close();
      byte[] bytes = bos.toByteArray();
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }


  public DAG() {
    this.confHolder = new ExternalizableConf(new Configuration(false));
  }

  public DAG(Configuration conf) {
    this.confHolder = new ExternalizableConf(conf);
  }

  private final static class InputPortMeta implements Serializable {
    private static final long serialVersionUID = 1L;

    private ExternalizableModule node;
    private Class<?> fieldDeclaringClass;
    private String fieldName;
    private ProtoInputPortFieldAnnotation portAnnotation;

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("node", this.node).
          append("portAnnotation", this.portAnnotation).
          append("field", this.fieldName).
          toString();
    }
  }

  private final static class OutputPortMeta implements Serializable {
    private static final long serialVersionUID = 1L;

    private ExternalizableModule node;
    private Class<?> fieldDeclaringClass;
    private String fieldName;
    private ProtoOutputPortFieldAnnotation portAnnotation;

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("node", this.node).
          append("portAnnotation", this.portAnnotation).
          append("field", this.fieldName).
          toString();
    }
  }

  public final class StreamDecl implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean inline;
    private final List<InputPortMeta> sinks = new ArrayList<InputPortMeta>();
    private OutputPortMeta source;
    private Class<? extends SerDe> serDeClass;
    private final String id;

    private StreamDecl(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    /**
     * Hint to manager that adjacent operators should be deployed in same container.
     * @return boolean
     */
    public boolean isInline() {
      return inline;
    }

    public StreamDecl setInline(boolean inline) {
      this.inline = inline;
      return this;
    }

    public Class<? extends SerDe> getSerDeClass() {
      return serDeClass;
    }

    public StreamDecl setSerDeClass(Class<? extends SerDe> serDeClass) {
      this.serDeClass = serDeClass;
      return this;
    }

    public OutputPortMeta getSource() {
      return source;
    }

    public StreamDecl setSource(Operator.OutputPort<?> port) {
      OperatorWrapper op = getOperatorWrapper(port.getOperator());
      OutputPortMeta portMeta = op.moduleHolder.outPortMap.get(port);
      if (portMeta == null) {
        throw new IllegalArgumentException("Invalid port reference " + port);
      }
      this.source = portMeta;
      if (op.outputStreams.containsKey(portMeta)) {
        String msg = String.format("Node %s already connected to %s", op.id, op.outputStreams.get(portMeta).id);
        throw new IllegalArgumentException(msg);
      }
      op.outputStreams.put(portMeta, this);
      return this;
    }

    public List<InputPortMeta> getSinks() {
      return sinks;
    }

    public StreamDecl addSink(Operator.InputPort<?> port) {
      OperatorWrapper op = getOperatorWrapper(port.getOperator());
      InputPortMeta portMeta = op.moduleHolder.inPortMap.get(port);
      if (portMeta == null) {
        throw new IllegalArgumentException("Invalid port reference " + port);
      }
      String portName = portMeta.portAnnotation.name();
      if (op.inputStreams.containsKey(portMeta)) {
        throw new IllegalArgumentException(String.format("Port %s already connected to stream %s", portName, op.inputStreams.get(portMeta)));
      }
      sinks.add(portMeta);
      op.inputStreams.put(portMeta, this);
      rootNodes.remove(portMeta.node);
      return this;
    }

  }

  private final class OperatorWrapper implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<InputPortMeta, StreamDecl> inputStreams = new HashMap<InputPortMeta, StreamDecl>();
    private final Map<OutputPortMeta, StreamDecl> outputStreams = new HashMap<OutputPortMeta, StreamDecl>();
//    private final Map<String, String> properties = new HashMap<String, String>();
    private final ExternalizableModule moduleHolder;
    private final String id;

    private transient Integer nindex; // for cycle detection
    private transient Integer lowlink; // for cycle detection

    private OperatorWrapper(Operator module) {
      this.moduleHolder = new ExternalizableModule();
      this.moduleHolder.set(module);
      this.id = module.getName();
    }

//    public Map<InputPortMeta, StreamDecl> getInputStreams() {
//      return this.inputStreams;
//    }

//    public Map<OutputPortMeta, StreamDecl> getOutputStreams() {
//      return this.outputStreams;
//    }

    public Operator getModule() {
      return this.moduleHolder.module;
    }

/*    *//**
     * Properties for the node.
     * @return Map<String, String>
     *//*
    public Map<String, String> getProperties() {
      return properties;
    }

    public OperatorWrapper setProperty(String name, String value) {
      properties.put(name, value);
      return this;
    }
*/
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", this.id).
          append("module", this.getModule().getClass().getName()).
          toString();
    }

  }

  public OperatorWrapper addOperator(Operator module) {
    if (nodes.containsKey(module.getName())) {
      throw new IllegalArgumentException("duplicate node id: " + nodes.get(module.getName()));
    }

    OperatorWrapper decl = new OperatorWrapper(module);
    rootNodes.add(decl);
    nodes.put(module.getName(), decl);

    return decl;
  }

  private OperatorWrapper getOperatorWrapper(Operator module) {
    // TODO: cache mapping
    for (OperatorWrapper o : getAllOperators()) {
      if (o.moduleHolder.module == module) {
        return o;
      }
    }
    if (module.getName() == null) {
      throw new IllegalArgumentException("Operator needs to have a name assigned: " + module);
    }

    return addOperator(module);
  }

  public StreamDecl addStream(String id) {
    StreamDecl s = this.streams.get(id);
    if (s == null) {
      s = new StreamDecl(id);
      this.streams.put(id, s);
    }
    return s;
  }

  /**
   * Add identified stream for given source and sinks.
   * @param id
   * @param source
   * @param sinks
   * @return
   */
  public StreamDecl addStream(String id, Operator.OutputPort<?> source, Operator.InputPort<?>... sinks) {
    StreamDecl s = addStream(id);
    s.setSource(source);
    for (Operator.InputPort<?> sink : sinks) {
      s.addSink(sink);
    }
    return s;
  }

  public StreamDecl getStream(String id) {
    return this.streams.get(id);
  }

  public List<OperatorWrapper> getRootOperators() {
     return Collections.unmodifiableList(this.rootNodes);
  }

  public Collection<OperatorWrapper> getAllOperators() {
    return Collections.unmodifiableCollection(this.nodes.values());
  }

  public Operator getOperator(String nodeId) {
    return this.nodes.get(nodeId).getModule();
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
    for (OperatorWrapper n : this.nodes.values()) {
      String className = n.getModule().getClass().getName();
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
   * Validate the topology. Includes checks that required ports are connected (TBD),
   * required configuration parameters specified, graph free of cycles etc.
   */
  public void validate() {
    // clear visited on all operators
    for (OperatorWrapper n : nodes.values()) {
      n.nindex = null;
      n.lowlink = null;
    }
    stack = new Stack<OperatorWrapper>();

    List<List<String>> cycles = new ArrayList<List<String>>();
    for (OperatorWrapper n : nodes.values()) {
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
  public void findStronglyConnected(OperatorWrapper n, List<List<String>> cycles) {
    n.nindex = nodeIndex;
    n.lowlink = nodeIndex;
    nodeIndex++;
    stack.push(n);

    // depth first successors traversal
    for (StreamDecl downStream : n.outputStreams.values()) {
      for (InputPortMeta sink : downStream.sinks) {
        OperatorWrapper successor = getOperatorWrapper(sink.node.get());
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

    // pop stack for all root operators
    if (n.lowlink.equals(n.nindex)) {
      List<String> connectedIds = new ArrayList<String>();
      while (!stack.isEmpty()) {
        OperatorWrapper n2 = stack.pop();
        connectedIds.add(n2.id);
        if (n2 == n) {
          break; // collected all connected operators
        }
      }
      // strongly connected (cycle) if more than one node in stack
      if (connectedIds.size() > 1) {
        LOG.debug("detected cycle from node {}: {}", n.id, connectedIds);
        cycles.add(connectedIds);
      }
    }
  }

  public static void write(DAG tplg, OutputStream os) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(os);
    oos.writeObject(tplg);
  }

  public static DAG read(InputStream is) throws IOException, ClassNotFoundException {
    return (DAG)new ObjectInputStream(is).readObject();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
        append("operators", this.nodes).
        append("streams", this.streams).
        append("properties", DAGPropertiesBuilder.toProperties(this.confHolder.conf)).
        toString();
  }

  public static void mapOutputPorts(ExternalizableModule operator, Map<Operator.OutputPort<?>, OutputPortMeta> metaPorts) {
    Field[] fields = operator.get().getClass().getDeclaredFields();
    for (int i = 0; i < fields.length; i++) {
      Field field = fields[i];
      ProtoOutputPortFieldAnnotation a = field.getAnnotation(ProtoOutputPortFieldAnnotation.class);
      if (a != null) {
        field.setAccessible(true);
        try {
          Object outPort = field.get(operator.get());
          if (outPort == null) {
            throw new IllegalArgumentException("port is null " + field);
          }
          if (!(outPort instanceof DefaultOutputPort)) {
            throw new IllegalArgumentException("port is not of type " + DefaultOutputPort.class.getName());
          }
          OutputPortMeta metaPort = new OutputPortMeta();
          metaPort.node = operator;
          metaPort.fieldName = field.getName();
          metaPort.fieldDeclaringClass = field.getDeclaringClass();
          metaPort.portAnnotation = a;
          metaPorts.put((DefaultOutputPort<?>)outPort, metaPort);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public static void mapInputPorts(ExternalizableModule operator, Map<Operator.InputPort<?>, InputPortMeta> metaPorts) {
    Field[] fields = operator.get().getClass().getDeclaredFields();
    for (int i = 0; i < fields.length; i++) {
      Field field = fields[i];
      ProtoInputPortFieldAnnotation a = field.getAnnotation(ProtoInputPortFieldAnnotation.class);
      if (a != null) {
        field.setAccessible(true);
        try {
          Object portObject = field.get(operator.get());
          if (portObject == null) {
            throw new IllegalArgumentException("port is null " + field);
          }
          if (!(portObject instanceof Operator.InputPort)) {
            throw new IllegalArgumentException("port is not of type " + Operator.InputPort.class.getName());
          }
          InputPortMeta metaPort = new InputPortMeta();
          metaPort.node = operator;
          metaPort.fieldName = field.getName();
          metaPort.fieldDeclaringClass = field.getDeclaringClass();
          metaPort.portAnnotation = a;
          metaPorts.put((Operator.InputPort<?>)portObject, metaPort);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}

/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan.logical;


import com.datatorrent.api.*;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.*;
import com.datatorrent.lib.util.FSStorageAgent;
import com.google.common.collect.Sets;
import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.validation.*;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * DAG contains the logical declarations of operators and streams.
 * <p>
 * Operators have ports that are connected through streams. Ports can be
 * mandatory or optional with respect to their need to connect a stream to it.
 * Each port can be connected to a single stream only. A stream has to be
 * connected to one output port and can go to multiple input ports.
 * <p>
 * The DAG will be serialized and deployed to the cluster, where it is translated
 * into the physical plan.
 *
 * @since 0.3.2
 */
public class LogicalPlan implements Serializable, DAG
{
  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  private static final long serialVersionUID = -2099729915606048704L;
  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlan.class);
  // The name under which the application master expects its configuration.
  public static final String SER_FILE_NAME = "dt-conf.ser";
  private static final transient AtomicInteger logicalOperatorSequencer = new AtomicInteger();

  /**
   * Constant
   * <code>SUBDIR_CHECKPOINTS="checkpoints"</code>
   */
  public static String SUBDIR_CHECKPOINTS = "checkpoints";
  /**
   * Constant
   * <code>SUBDIR_STATS="stats"</code>
   */
  public static String SUBDIR_STATS = "stats";
  /**
   * Constant
   * <code>SUBDIR_EVENTS="events"</code>
   */
  public static String SUBDIR_EVENTS = "events";

  /**
   * A flag to specify whether to use the fast publisher or not. This attribute was moved
   * from DAGContext. This can be here till the fast publisher is fully tested and working as desired.
   * Then it can be moved back to DAGContext.
   */
  public static Attribute<Boolean> FAST_PUBLISHER_SUBSCRIBER = new Attribute<Boolean>(false);
  public static Attribute<String> LICENSE = new Attribute<String>((String)null, new StringCodec.String2String());
  public static Attribute<String> LICENSE_ROOT = new Attribute<String>((String)null, new StringCodec.String2String());

  static {
    Attribute.AttributeMap.AttributeInitializer.initialize(LogicalPlan.class);
  }

  private final Map<String, StreamMeta> streams = new HashMap<String, StreamMeta>();
  private final Map<String, OperatorMeta> operators = new HashMap<String, OperatorMeta>();
  private final List<OperatorMeta> rootOperators = new ArrayList<OperatorMeta>();
  private final Attribute.AttributeMap attributes = new DefaultAttributeMap();
  private transient int nodeIndex = 0; // used for cycle validation
  private transient Stack<OperatorMeta> stack = new Stack<OperatorMeta>(); // used for cycle validation

  @Override
  public Attribute.AttributeMap getAttributes()
  {
    return attributes;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    T val = attributes.get(key);
    if (val == null) {
      return key.defaultValue;
    }

    return val;
  }

  public LogicalPlan()
  {
  }

  @Override
  public void setCounters(Object counters)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public final class InputPortMeta implements DAG.InputPortMeta, Serializable
  {
    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 1L;
    private OperatorMeta operatorMeta;
    private String fieldName;
    private InputPortFieldAnnotation portAnnotation;
    private final Attribute.AttributeMap attributes = new DefaultAttributeMap();
    private String tupleTypeString;

    public OperatorMeta getOperatorWrapper()
    {
      return operatorMeta;
    }

    public String getPortName()
    {
      return fieldName;
    }

    public InputPort<?> getPortObject() {
      for (Map.Entry<InputPort<?>, InputPortMeta> e : operatorMeta.getPortMapping().inPortMap.entrySet()) {
        if (e.getValue() == this) {
          return e.getKey();
        }
      }
      throw new AssertionError("Cannot find the port object for " + this);
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
              append("operator", this.operatorMeta).
              append("portAnnotation", this.portAnnotation).
              append("field", this.fieldName).
              toString();
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return attributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T attr = attributes.get(key);
      if (attr == null) {
        return key.defaultValue;
      }

      return attr;
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Gets the tuple type.
     * Note that this can be slow, because we need the port meta to be serializable
     *
     * @return the tuple type
     */
    public Type getTupleType()
    {
      Operator operator = this.operatorMeta.operator;
      for (Class<?> c = operator.getClass(); c != Object.class; c = c.getSuperclass()) {
        Field[] fields = c.getDeclaredFields();
        for (Field field : fields) {
          field.setAccessible(true);

          try {
            Object portObject = field.get(operator);

            if (portObject instanceof OutputPort) {
              if (field.getName().equals(this.fieldName)) {
                return getPortType(field);
              }
            }
          }
          catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      }
      throw new RuntimeException("Tuple type cannot be determined.");
    }


    public String getTupleTypeString()
    {
      return tupleTypeString;
    }

  }

  public final class OutputPortMeta implements DAG.OutputPortMeta, Serializable
  {
    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 1L;
    private OperatorMeta operatorMeta;
    private String fieldName;
    private OutputPortFieldAnnotation portAnnotation;
    private final DefaultAttributeMap attributes = new DefaultAttributeMap();
    private String tupleTypeString;

    public OperatorMeta getOperatorWrapper()
    {
      return operatorMeta;
    }

    public String getPortName()
    {
      return fieldName;
    }

    public OutputPort<?> getPortObject() {
      for (Map.Entry<OutputPort<?>, OutputPortMeta> e : operatorMeta.getPortMapping().outPortMap.entrySet()) {
        if (e.getValue() == this) {
          return e.getKey();
        }
      }
      throw new AssertionError("Cannot find the port object for " + this);
    }

    public Operator.Unifier<?> getUnifier() {
      for (Map.Entry<OutputPort<?>, OutputPortMeta> e : operatorMeta.getPortMapping().outPortMap.entrySet()) {
        if (e.getValue() == this) {
          Unifier<?> unifier = e.getKey().getUnifier();
          return unifier;
        }
      }

      return null;
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return attributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T attr = attributes.get(key);
      if (attr == null) {
        return key.defaultValue;
      }

      return attr;
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
              append("operator", this.operatorMeta).
              append("portAnnotation", this.portAnnotation).
              append("field", this.fieldName).
              toString();
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public String getTupleTypeString()
    {
      return tupleTypeString;
    }
    /**
     * Gets the tuple type.
     * Note that this can be slow, because we need the port meta to be serializable
     *
     * @return the tuple type
     */
    public Type getTupleType()
    {
      Operator operator = this.operatorMeta.operator;
      for (Class<?> c = operator.getClass(); c != Object.class; c = c.getSuperclass()) {
        Field[] fields = c.getDeclaredFields();
        for (Field field : fields) {
          field.setAccessible(true);

          try {
            Object portObject = field.get(operator);

            if (portObject instanceof InputPort) {
              if (field.getName().equals(this.fieldName)) {
                return getPortType(field);
              }
            }
          }
          catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      }
      throw new RuntimeException("Tuple type cannot be determined.");
    }
  }

  /**
   * Representation of streams in the logical layer. Instances are created through {@link LogicalPlan#addStream}.
   */
  public final class StreamMeta implements DAG.StreamMeta, Serializable
  {
    private static final long serialVersionUID = 1L;
    private Locality locality;
    private final List<InputPortMeta> sinks = new ArrayList<InputPortMeta>();
    private OutputPortMeta source;
    private final String id;

    private StreamMeta(String id)
    {
      this.id = id;
    }

    @Override
    public String getName()
    {
      return id;
    }

    @Override
    public Locality getLocality() {
      return this.locality;
    }

    @Override
    public StreamMeta setLocality(Locality locality) {
      this.locality = locality;
      return this;
    }

    public OutputPortMeta getSource()
    {
      return source;
    }

    @Override
    public StreamMeta setSource(Operator.OutputPort<?> port)
    {
      OutputPortMeta portMeta = assertGetPortMeta(port);
      OperatorMeta om = portMeta.getOperatorWrapper();
      if (om.outputStreams.containsKey(portMeta)) {
        String msg = String.format("Operator %s already connected to %s", om.name, om.outputStreams.get(portMeta).id);
        throw new IllegalArgumentException(msg);
      }
      this.source = portMeta;
      om.outputStreams.put(portMeta, this);
      return this;
    }

    public List<InputPortMeta> getSinks()
    {
      return sinks;
    }

    @Override
    public StreamMeta addSink(Operator.InputPort<?> port)
    {
      InputPortMeta portMeta = assertGetPortMeta(port);
      OperatorMeta om = portMeta.getOperatorWrapper();
      String portName = portMeta.getPortName();
      if (om.inputStreams.containsKey(portMeta)) {
        throw new IllegalArgumentException(String.format("Port %s already connected to stream %s", portName, om.inputStreams.get(portMeta)));
      }

      /*
      finalizeValidate(portMeta);
      */

      sinks.add(portMeta);
      om.inputStreams.put(portMeta, this);
      rootOperators.remove(portMeta.operatorMeta);

      return this;
    }

    public void remove() {
      for (InputPortMeta ipm : this.sinks) {
        ipm.getOperatorWrapper().inputStreams.remove(ipm);
        if (ipm.getOperatorWrapper().inputStreams.isEmpty()) {
          rootOperators.add(ipm.getOperatorWrapper());
        }
      }
      this.sinks.clear();
      if (this.source != null) {
        this.source.getOperatorWrapper().outputStreams.remove(this.source);
      }
      this.source = null;
      streams.remove(this.id);
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
              append("id", this.id).
              toString();
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 31 * hash + (this.locality != null ? this.locality.hashCode() : 0);
      hash = 31 * hash + (this.source != null ? this.source.hashCode() : 0);
      hash = 31 * hash + (this.id != null ? this.id.hashCode() : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final StreamMeta other = (StreamMeta)obj;
      if (this.locality != other.locality) {
        return false;
      }
      if (this.sinks != other.sinks && (this.sinks == null || !this.sinks.equals(other.sinks))) {
        return false;
      }
      if (this.source != other.source && (this.source == null || !this.source.equals(other.source))) {
        return false;
      }
      if ((this.id == null) ? (other.id != null) : !this.id.equals(other.id)) {
        return false;
      }
      return true;
    }

  }

  /**
   * Operator meta object.
   */
  public final class OperatorMeta implements DAG.OperatorMeta, Serializable
  {
    private final LinkedHashMap<InputPortMeta, StreamMeta> inputStreams = new LinkedHashMap<InputPortMeta, StreamMeta>();
    private final LinkedHashMap<OutputPortMeta, StreamMeta> outputStreams = new LinkedHashMap<OutputPortMeta, StreamMeta>();
    private final Attribute.AttributeMap attributes = new DefaultAttributeMap();
    @SuppressWarnings("unused")
    private final int id;
    @NotNull
    private final String name;
    private final OperatorAnnotation operatorAnnotation;
    private final LogicalOperatorStatus status;
    private transient Integer nindex; // for cycle detection
    private transient Integer lowlink; // for cycle detection
    private transient Operator operator;
    /*
     * Used for  OIO validation,
     *  value null => node not visited yet
     *  value -1  => node visited and not oio
     *  other value => represents the root oio node for this node
     */
    private transient Integer oioRoot = null;

    private OperatorMeta(String name, Operator operator)
    {
      LOG.debug("Initializing {} as {}", name, operator.getClass().getName());
      this.operatorAnnotation = operator.getClass().getAnnotation(OperatorAnnotation.class);
      this.name = name;
      this.operator = operator;
      this.id = logicalOperatorSequencer.decrementAndGet();
      this.status = new LogicalOperatorStatus(name);
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public Attribute.AttributeMap getAttributes()
    {
      return attributes;
    }

    @Override
    public <T> T getValue(Attribute<T> key)
    {
      T attr = attributes.get(key);
      if (attr == null) {
        return key.defaultValue;
      }

      return attr;
    }

    public <T> T getValue2(Attribute<T> key)
    {
      T attr = attributes.get(key);
      if (attr == null) {
        return LogicalPlan.this.getValue(key);
      }

      return attr;
    }

    public LogicalOperatorStatus getStatus()
    {
      return status;
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
      //getValue2(OperatorContext.STORAGE_AGENT).save(operator, id, Checkpoint.STATELESS_CHECKPOINT_WINDOW_ID);
      out.defaultWriteObject();
      FSStorageAgent.store(out, operator);
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException
    {
      input.defaultReadObject();
      // TODO: not working because - we don't have the storage agent in parent attribuet map
      //operator = (Operator)getValue2(OperatorContext.STORAGE_AGENT).load(id, Checkpoint.STATELESS_CHECKPOINT_WINDOW_ID);
      operator = (Operator)FSStorageAgent.retrieve(input);
    }

    @Override
    public void setCounters(Object counters)
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private class PortMapping implements Operators.OperatorDescriptor
    {
      private final Map<Operator.InputPort<?>, InputPortMeta> inPortMap = new HashMap<Operator.InputPort<?>, InputPortMeta>();
      private final Map<Operator.OutputPort<?>, OutputPortMeta> outPortMap = new HashMap<Operator.OutputPort<?>, OutputPortMeta>();
      private final Map<String, Object> portNameMap = new HashMap<String, Object>();

      @Override
      public void addInputPort(InputPort<?> portObject, Field field, InputPortFieldAnnotation a)
      {
        if (!OperatorMeta.this.inputStreams.isEmpty()) {
          for (Map.Entry<LogicalPlan.InputPortMeta, LogicalPlan.StreamMeta> e : OperatorMeta.this.inputStreams.entrySet()) {
            LogicalPlan.InputPortMeta pm = e.getKey();
            if (pm.operatorMeta == OperatorMeta.this && pm.fieldName.equals(field.getName())) {
              //LOG.debug("Found existing port meta for: " + field);
              inPortMap.put(portObject, pm);
              checkDuplicateName(pm.getPortName(), pm);
              return;
            }
          }
        }
        InputPortMeta metaPort = new InputPortMeta();
        metaPort.operatorMeta = OperatorMeta.this;
        metaPort.fieldName = field.getName();
        metaPort.portAnnotation = a;
        Type portType = getPortType(field);
        if (portType != null) {
          metaPort.tupleTypeString = portType.toString();
        }
        inPortMap.put(portObject, metaPort);
        checkDuplicateName(metaPort.getPortName(), metaPort);
      }

      @Override
      public void addOutputPort(OutputPort<?> portObject, Field field, OutputPortFieldAnnotation a)
      {
        if (!OperatorMeta.this.outputStreams.isEmpty()) {
          for (Map.Entry<LogicalPlan.OutputPortMeta, LogicalPlan.StreamMeta> e : OperatorMeta.this.outputStreams.entrySet()) {
            LogicalPlan.OutputPortMeta pm = e.getKey();
            if (pm.operatorMeta == OperatorMeta.this && pm.fieldName.equals(field.getName())) {
              //LOG.debug("Found existing port meta for: " + field);
              outPortMap.put(portObject, pm);
              checkDuplicateName(pm.getPortName(), pm);
              return;
            }
          }
        }
        OutputPortMeta metaPort = new OutputPortMeta();
        metaPort.operatorMeta = OperatorMeta.this;
        metaPort.fieldName = field.getName();
        metaPort.portAnnotation = a;
        Type portType = getPortType(field);
        if (portType != null) {
          metaPort.tupleTypeString = portType.toString();
        }
        outPortMap.put(portObject, metaPort);
        checkDuplicateName(metaPort.getPortName(), metaPort);
      }

      private void checkDuplicateName(String portName, Object portMeta) {
        Object existingValue = portNameMap.put(portName, portMeta);
        if (existingValue != null) {
          String msg = String.format("Port name %s of %s duplicates %s", portName, portMeta, existingValue);
          throw new IllegalArgumentException(msg);
        }
      }
    }
    /**
     * Ports objects are transient, we keep a lazy initialized mapping
     */
    private transient PortMapping portMapping = null;

    private PortMapping getPortMapping()
    {
      if (this.portMapping == null) {
        this.portMapping = new PortMapping();
        Operators.describe(this.getOperator(), portMapping);
      }
      return portMapping;
    }

    @Override
    public OutputPortMeta getMeta(Operator.OutputPort<?> port)
    {
      return getPortMapping().outPortMap.get(port);
    }

    @Override
    public InputPortMeta getMeta(Operator.InputPort<?> port)
    {
      return getPortMapping().inPortMap.get(port);
    }

    public Map<InputPortMeta, StreamMeta> getInputStreams()
    {
      return this.inputStreams;
    }

    public Map<OutputPortMeta, StreamMeta> getOutputStreams()
    {
      return this.outputStreams;
    }

    @Override
    public Operator getOperator()
    {
      return operator;
    }

    public LogicalPlan getDAG()
    {
      return LogicalPlan.this;
    }

    @Override
    public String toString()
    {
      return "OperatorMeta{" + "name=" + name + ", operator=" + operator + ", attributes=" + attributes + '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof OperatorMeta)) {
        return false;
      }

      OperatorMeta that = (OperatorMeta) o;

      if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) {
        return false;
      }
      if (!name.equals(that.name)) {
        return false;
      }
      if (operatorAnnotation != null ? !operatorAnnotation.equals(that.operatorAnnotation) : that.operatorAnnotation != null) {
        return false;
      }
      if (operator != null ? !operator.equals(that.operator) : that.operator != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return name.hashCode();
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201401091635L;
  }

  @Override
  public <T extends Operator> T addOperator(String name, Class<T> clazz)
  {
    T instance;
    try {
      instance = clazz.newInstance();
    } catch (Exception ex) {
      throw new IllegalArgumentException(ex);
    }
    addOperator(name, instance);
    return instance;
  }

  @Override
  public <T extends Operator> T addOperator(String name, T operator)
  {
    // TODO: optional interface to provide contextual information to instance
    if (operator instanceof BaseOperator) {
      ((BaseOperator)operator).setName(name);
    }
    if (operators.containsKey(name)) {
      if (operators.get(name) == (Object)operator) {
        return operator;
      }
      throw new IllegalArgumentException("duplicate operator id: " + operators.get(name));
    }

    OperatorMeta decl = new OperatorMeta(name, operator);
    rootOperators.add(decl); // will be removed when a sink is added to an input port for this operator
    operators.put(name, decl);
    return operator;
  }

  public void removeOperator(Operator operator)
  {
    OperatorMeta om = getMeta(operator);
    if (om == null) {
      return;
    }

    Map<InputPortMeta, StreamMeta> inputStreams = om.getInputStreams();
    for (Map.Entry<InputPortMeta, StreamMeta> e : inputStreams.entrySet()) {
      if (e.getKey().getOperatorWrapper() == om) {
         e.getValue().sinks.remove(e.getKey());
      }
    }
    this.operators.remove(om.getName());
    rootOperators.remove(om);
  }

  @Override
  public StreamMeta addStream(String id)
  {
    StreamMeta s = new StreamMeta(id);
    StreamMeta o = streams.put(id, s);
    if (o == null) {
      return s;
    }

    throw new IllegalArgumentException("duplicate stream id: " + o);
  }

  @Override
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>... sinks)
  {
    StreamMeta s = addStream(id);
    s.setSource(source);
    for (Operator.InputPort<?> sink: sinks) {
      s.addSink(sink);
    }
    return s;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1)
  {
    @SuppressWarnings("rawtypes")
    InputPort[] ports = new Operator.InputPort[]{sink1};
    return addStream(id, source, ports);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1, Operator.InputPort<? super T> sink2)
  {
    @SuppressWarnings("rawtypes")
    InputPort[] ports = new Operator.InputPort[] {sink1, sink2};
    return addStream(id, source, ports);
  }

  public StreamMeta getStream(String id)
  {
    return this.streams.get(id);
  }

  /**
   * Set attribute for the operator. For valid attributes, see {
   *
   * @param operator
   * @return AttributeMap<OperatorContext>
   */
  public Attribute.AttributeMap getContextAttributes(Operator operator)
  {
    return getMeta(operator).attributes;
  }

  @Override
  public <T> void setAttribute(Attribute<T> key, T value)
  {
    this.getAttributes().put(key, value);
  }

  @Override
  public <T> void setAttribute(Operator operator, Attribute<T> key, T value)
  {
    this.getMeta(operator).attributes.put(key, value);
  }

  private OutputPortMeta assertGetPortMeta(Operator.OutputPort<?> port)
  {
    for (OperatorMeta o: getAllOperators()) {
      OutputPortMeta opm = o.getPortMapping().outPortMap.get(port);
      if (opm != null) {
        return opm;
      }
    }
    throw new IllegalArgumentException("Port is not associated to any operator in the DAG: " + port);
  }

  private InputPortMeta assertGetPortMeta(Operator.InputPort<?> port)
  {
    for (OperatorMeta o: getAllOperators()) {
      InputPortMeta opm = o.getPortMapping().inPortMap.get(port);
      if (opm != null) {
        return opm;
      }
    }
    throw new IllegalArgumentException("Port is not associated to any operator in the DAG: " + port);
  }

  @Override
  public <T> void setOutputPortAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value)
  {
    assertGetPortMeta(port).attributes.put(key, value);
  }

  @Override
  public <T> void setInputPortAttribute(Operator.InputPort<?> port, Attribute<T> key, T value)
  {
    assertGetPortMeta(port).attributes.put(key, value);
  }

  public List<OperatorMeta> getRootOperators()
  {
    return Collections.unmodifiableList(this.rootOperators);
  }

  public Collection<OperatorMeta> getAllOperators()
  {
    return Collections.unmodifiableCollection(this.operators.values());
  }

  public Collection<StreamMeta> getAllStreams()
  {
    return Collections.unmodifiableCollection(this.streams.values());
  }

  @Override
  public OperatorMeta getOperatorMeta(String operatorName)
  {
    return this.operators.get(operatorName);
  }

  @Override
  public OperatorMeta getMeta(Operator operator)
  {
    // TODO: cache mapping
    for (OperatorMeta o: getAllOperators()) {
      if (o.operator == operator) {
        return o;
      }
    }
    throw new IllegalArgumentException("Operator not associated with the DAG: " + operator);
  }

  public int getMaxContainerCount()
  {
    return this.getValue(CONTAINERS_MAX_COUNT);
  }

  public boolean isDebug()
  {
    return this.getValue(DEBUG);
  }

  public int getMasterMemoryMB()
  {
    return this.getValue(MASTER_MEMORY_MB);
  }

  public String assertAppPath()
  {
    String path = getAttributes().get(LogicalPlan.APPLICATION_PATH);
    if (path == null) {
      throw new AssertionError("Missing " + LogicalPlan.APPLICATION_PATH);
    }
    return path;
  }

  /**
   * Class dependencies for the topology. Used to determine jar file dependencies.
   *
   * @return Set<String>
   */
  public Set<String> getClassNames()
  {
    Set<String> classNames = new HashSet<String>();
    for (OperatorMeta n: this.operators.values()) {
      String className = n.getOperator().getClass().getName();
      if (className != null) {
        classNames.add(className);
      }
    }
    for (StreamMeta n: this.streams.values()) {
      for (InputPortMeta sink : n.getSinks()) {
        StreamCodec<?> streamCodec = sink.getValue(PortContext.STREAM_CODEC);
        if (streamCodec != null) {
          classNames.add(streamCodec.getClass().getName());
        } else {
          Class<? extends StreamCodec<?>> codecClass = sink.getPortObject().getStreamCodec();
          if (codecClass != null) {
            classNames.add(codecClass.getName());
          }
        }
      }
    }
    return classNames;
  }

  /**
   * Validate the plan. Includes checks that required ports are connected,
   * required configuration parameters specified, graph free of cycles etc.
   *
   * @throws ConstraintViolationException
   */
  public void validate() throws ConstraintViolationException
  {
    ValidatorFactory factory =
            Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    // clear oioRoot values in all operators
    for (OperatorMeta n: operators.values()) {
      n.oioRoot = null;
    }

    // clear visited on all operators
    for (OperatorMeta n: operators.values()) {
      n.nindex = null;
      n.lowlink = null;

      // validate configuration
      Set<ConstraintViolation<Operator>> constraintViolations = validator.validate(n.getOperator());
      if (!constraintViolations.isEmpty()) {
        Set<ConstraintViolation<?>> copySet = new HashSet<ConstraintViolation<?>>(constraintViolations.size());
        // workaround bug in ConstraintViolationException constructor
        // (should be public <T> ConstraintViolationException(String message, Set<ConstraintViolation<T>> constraintViolations) { ... })
        for (ConstraintViolation<Operator> cv: constraintViolations) {
          copySet.add(cv);
        }
        throw new ConstraintViolationException("Operator " + n.getName() + " violates constraints " + copySet, copySet);
      }

      OperatorMeta.PortMapping portMapping = n.getPortMapping();

      // Check operator annotation
      if (n.operatorAnnotation != null) {
        // Check if partition property of the operator is being honored
        if (!n.operatorAnnotation.partitionable()) {
          // Check if INITIAL_PARTITION_COUNT is set
          int partitionCount = n.getValue(OperatorContext.INITIAL_PARTITION_COUNT);
          if (partitionCount > 1) {
            throw new ValidationException("Operator " + n.getName() + " is not partitionable but INITIAL_PARTITION_COUNT attribute is set" );
          } else {
            // Check if any of the input ports have partition attributes set
            for (InputPortMeta pm : portMapping.inPortMap.values()) {
              Boolean paralellPartition = pm.getValue(PortContext.PARTITION_PARALLEL);
              if (paralellPartition) {
                throw new ValidationException("Operator " + n.getName() + " is not partitionable but PARTITION_PARALLEL attribute is set" );
              }
            }
          }

          // Check if the operator implements Partitioner
          if (n.getValue(OperatorContext.PARTITIONER) != null
              || n.attributes != null && !n.attributes.contains(OperatorContext.PARTITIONER) && Partitioner.class.isAssignableFrom(n.getOperator().getClass())) {
            throw new ValidationException("Operator " + n.getName() + " provides partitioning capabilities but the annotation on the operator class declares it non partitionable!");
          }
        }

        //If operator can not be check-pointed in middle of application window then the checkpoint window count should be
        // a multiple of application window count
        if (!n.operatorAnnotation.checkpointableWithinAppWindow()) {
          if (n.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT) % n.getValue(OperatorContext.APPLICATION_WINDOW_COUNT) != 0) {
            throw new ValidationException("Operator " + n.getName() + " cannot be check-pointed between an application window " +
              "but the checkpoint-window-count " + n.getValue(OperatorContext.CHECKPOINT_WINDOW_COUNT) +
              " is not a multiple application-window-count " + n.getValue(OperatorContext.APPLICATION_WINDOW_COUNT));
          }
        }
      }

      // check that non-optional ports are connected
      for (InputPortMeta pm: portMapping.inPortMap.values()) {
        StreamMeta sm = n.inputStreams.get(pm);
        if (sm == null) {
          if (pm.portAnnotation == null || !pm.portAnnotation.optional()) {
            throw new ValidationException("Input port connection required: " + n.name + "." + pm.getPortName());
          }
        } else {
          // check locality constraints
          DAG.Locality locality = sm.getLocality();
          if (locality == DAG.Locality.THREAD_LOCAL) {
            if (n.inputStreams.size() > 1) {
              validateThreadLocal(n);
            }
          }
        }
      }

      boolean allPortsOptional = true;
      for (OutputPortMeta pm: portMapping.outPortMap.values()) {
        if (!n.outputStreams.containsKey(pm)) {
          if (pm.portAnnotation != null && !pm.portAnnotation.optional()) {
            throw new ValidationException("Output port connection required: " + n.name + "." + pm.getPortName());
          }
        }
        allPortsOptional &= (pm.portAnnotation != null && pm.portAnnotation.optional());
      }
      if (!allPortsOptional && n.outputStreams.isEmpty()) {
        throw new ValidationException("At least one output port must be connected: " + n.name);
      }
    }
    stack = new Stack<OperatorMeta>();

    List<List<String>> cycles = new ArrayList<List<String>>();
    for (OperatorMeta n: operators.values()) {
      if (n.nindex == null) {
        findStronglyConnected(n, cycles);
      }
    }
    if (!cycles.isEmpty()) {
      throw new ValidationException("Loops in graph: " + cycles);
    }

    for (StreamMeta s: streams.values()) {
      if (s.source == null || (s.sinks.isEmpty())) {
        throw new ValidationException(String.format("stream not connected: %s", s.getName()));
      }
    }

    // processing mode
    Set<OperatorMeta> visited = Sets.newHashSet();
    for (OperatorMeta om : this.rootOperators) {
      validateProcessingMode(om, visited);
    }

    handleDeprecation();
  }

  @SuppressWarnings("deprecation")
  private void handleDeprecation()
  {
    if (attributes.contains(DAGContext.CONTAINER_MEMORY_MB)) {
      LOG.warn("{} is deprecated, use {} instead", DAGContext.CONTAINER_MEMORY_MB, OperatorContext.MEMORY_MB);
      attributes.put(OperatorContext.MEMORY_MB, attributes.get(CONTAINER_MEMORY_MB));
    }
  }

  /*
   * Validates OIO constraints for operators with more than one input streams
   * For a node to be OIO,
   *  1. all its input streams should be OIO
   *  2. all its input streams should have OIO from single source node
   */
  private void validateThreadLocal(OperatorMeta om) {
    Integer oioRoot = null;

    // already visited and validated
    if (om.oioRoot != null) {
      return;
    }

    for (StreamMeta sm: om.inputStreams.values()){
      // validation fail as each input stream should be OIO
      if (sm.locality != Locality.THREAD_LOCAL){
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as at least one of the input streams is not %s",
                                   Locality.THREAD_LOCAL, om, Locality.THREAD_LOCAL);
        throw new ValidationException(msg);
      }

      // gets oio root for input operator for the stream
      Integer oioStreamRoot = getOioRoot(sm.source.operatorMeta);

      // validation fail as each input stream should have a common OIO root
      if (oioStreamRoot == -1){
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as at least one of the input streams is not originating from common OIO owner node",
                                   Locality.THREAD_LOCAL, om, Locality.THREAD_LOCAL);
        throw new ValidationException(msg);
      }

      // populate oioRoot with root OIO node id for first stream, then validate for subsequent streams to have same root OIO node
      if (oioRoot == null) {
        oioRoot = oioStreamRoot;
      } else if (oioRoot.intValue() != oioStreamRoot.intValue()) {
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as they origin from different owner OIO operators", sm.locality, om);
        throw new ValidationException(msg);
      }
    }

    om.oioRoot = oioRoot;
  }

  /**
   * Helper method for validateThreadLocal method, runs recursively
   * For a given operator, visits all upstream operators in DFS, validates and marks them as visited
   * returns hashcode of owner oio node if it exists, else returns -1
   */
  private Integer getOioRoot(OperatorMeta om) {
    // operators which were already marked a visited
    if (om.oioRoot != null){
      return om.oioRoot;
    }

    // operators which were not visited before
    switch (om.inputStreams.size()) {
      case 1:
        StreamMeta sm = om.inputStreams.values().iterator().next();
        if (sm.locality == Locality.THREAD_LOCAL) {
          Integer oioStreamRoot = getOioRoot(sm.source.operatorMeta);
          if (oioStreamRoot == -1) {
            om.oioRoot = sm.source.operatorMeta.hashCode();
          }
          else {
            om.oioRoot = oioStreamRoot;
          }
          return om.oioRoot;
        }
        else {
          om.oioRoot = -1;
          return om.hashCode();
        }
      case 0:
        om.oioRoot = -1;
        return om.hashCode();

      default:
        validateThreadLocal(om);
        return om.oioRoot;
    }
  }

  /**
   * Check for cycles in the graph reachable from start node n. This is done by
   * attempting to find strongly connected components.
   *
   * @see <a href="http://en.wikipedia.org/wiki/Tarjan%E2%80%99s_strongly_connected_components_algorithm">http://en.wikipedia.org/wiki/Tarjan%E2%80%99s_strongly_connected_components_algorithm</a>
   *
   * @param om
   * @param cycles
   */
  public void findStronglyConnected(OperatorMeta om, List<List<String>> cycles)
  {
    om.nindex = nodeIndex;
    om.lowlink = nodeIndex;
    nodeIndex++;
    stack.push(om);

    // depth first successors traversal
    for (StreamMeta downStream: om.outputStreams.values()) {
      for (InputPortMeta sink: downStream.sinks) {
        OperatorMeta successor = sink.getOperatorWrapper();
        if (successor == null) {
          continue;
        }
        // check for self referencing node
        if (om == successor) {
          cycles.add(Collections.singletonList(om.name));
        }
        if (successor.nindex == null) {
          // not visited yet
          findStronglyConnected(successor, cycles);
          om.lowlink = Math.min(om.lowlink, successor.lowlink);
        }
        else if (stack.contains(successor)) {
          om.lowlink = Math.min(om.lowlink, successor.nindex);
        }
      }
    }

    // pop stack for all root operators
    if (om.lowlink.equals(om.nindex)) {
      List<String> connectedIds = new ArrayList<String>();
      while (!stack.isEmpty()) {
        OperatorMeta n2 = stack.pop();
        connectedIds.add(n2.name);
        if (n2 == om) {
          break; // collected all connected operators
        }
      }
      // strongly connected (cycle) if more than one node in stack
      if (connectedIds.size() > 1) {
        LOG.debug("detected cycle from node {}: {}", om.name, connectedIds);
        cycles.add(connectedIds);
      }
    }
  }

  private void validateProcessingMode(OperatorMeta om, Set<OperatorMeta> visited)
  {
    for (StreamMeta is : om.getInputStreams().values()) {
      if (!visited.contains(is.getSource().getOperatorWrapper())) {
        // process all inputs first
        return;
      }
    }
    visited.add(om);
    Operator.ProcessingMode pm = om.getValue(OperatorContext.PROCESSING_MODE);
    for (StreamMeta os : om.outputStreams.values()) {
      for (InputPortMeta sink: os.sinks) {
        OperatorMeta sinkOm = sink.getOperatorWrapper();
        Operator.ProcessingMode sinkPm = sinkOm.attributes == null? null: sinkOm.attributes.get(OperatorContext.PROCESSING_MODE);
        if (sinkPm == null) {
          // If the source processing mode is AT_MOST_ONCE and a processing mode is not specified for the sink then set it to AT_MOST_ONCE as well
          if (Operator.ProcessingMode.AT_MOST_ONCE.equals(pm)) {
            LOG.warn("Setting processing mode for operator {} to {}", sinkOm.getName(), pm);
            sinkOm.getAttributes().put(OperatorContext.PROCESSING_MODE, pm);
          } else if (Operator.ProcessingMode.EXACTLY_ONCE.equals(pm)) {
            // If the source processing mode is EXACTLY_ONCE and a processing mode is not specified for the sink then throw a validation error
            String msg = String.format("Processing mode for %s should be AT_MOST_ONCE for source %s/%s", sinkOm.getName(), om.getName(), pm);
            throw new ValidationException(msg);
          }
        } else {
          /*
           * If the source processing mode is AT_MOST_ONCE and the processing mode for the sink is not AT_MOST_ONCE throw a validation error
           * If the source processing mode is EXACTLY_ONCE and the processing mode for the sink is not AT_MOST_ONCE throw a validation error
           */
          if ((Operator.ProcessingMode.AT_MOST_ONCE.equals(pm) && (sinkPm != pm))
                  || (Operator.ProcessingMode.EXACTLY_ONCE.equals(pm) && !Operator.ProcessingMode.AT_MOST_ONCE.equals(sinkPm))) {
            String msg = String.format("Processing mode %s/%s not valid for source %s/%s", sinkOm.getName(), sinkPm, om.getName(), pm);
            throw new ValidationException(msg);
          }
        }
        validateProcessingMode(sinkOm, visited);
      }
    }
  }

  public static void write(DAG dag, OutputStream os) throws IOException
  {
    ObjectOutputStream oos = new ObjectOutputStream(os);
    oos.writeObject(dag);
  }

  public static LogicalPlan read(InputStream is) throws IOException, ClassNotFoundException
  {
    return (LogicalPlan)new ObjectInputStream(is).readObject();
  }


  public static Type getPortType(Field f)
  {
    if (f.getGenericType() instanceof ParameterizedType) {
      ParameterizedType t = (ParameterizedType)f.getGenericType();
      //LOG.debug("Field type is parameterized: " + Arrays.asList(t.getActualTypeArguments()));
      //LOG.debug("rawType: " + t.getRawType()); // the port class
      Type typeArgument = t.getActualTypeArguments()[0];
      if (typeArgument instanceof Class) {
        return typeArgument;
      }
      else if (typeArgument instanceof TypeVariable) {
        TypeVariable<?> tv = (TypeVariable<?>)typeArgument;
        LOG.debug("bounds: " + Arrays.asList(tv.getBounds()));
        // variable may contain other variables, java.util.Map<java.lang.String, ? extends T2>
        return tv.getBounds()[0];
      }
      else if (typeArgument instanceof GenericArrayType) {
        LOG.debug("type {} is of GenericArrayType", typeArgument);
        return typeArgument;
      }
      else if (typeArgument instanceof WildcardType) {
        LOG.debug("type {} is of WildcardType", typeArgument);
        return typeArgument;
      }
      else if (typeArgument instanceof ParameterizedType) {
        return typeArgument;
      }
      else {
        LOG.error("Type argument is of expected type {}", typeArgument);
        return null;
      }
    }
    else {
      // ports are always parameterized
      LOG.error("No type variable: {}, typeParameters: {}", f.getType(), Arrays.asList(f.getClass().getTypeParameters()));
      return null;
    }
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
            append("operators", this.operators).
            append("streams", this.streams).
            append("properties", this.attributes).
            toString();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LogicalPlan)) {
      return false;
    }

    LogicalPlan that = (LogicalPlan) o;

    if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) {
      return false;
    }
    if (streams != null ? !streams.equals(that.streams) : that.streams != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = streams != null ? streams.hashCode() : 0;
    result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
    return result;
  }
}

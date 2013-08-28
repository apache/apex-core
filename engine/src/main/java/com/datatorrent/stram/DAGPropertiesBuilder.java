/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.Operators;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.google.common.collect.Maps;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import org.apache.commons.beanutils.BeanMap;

/**
 *
 * Builder for the DAG logical representation of operators and streams from properties.<p>
 * <br>
 * Supports reading as name-value pairs from Hadoop {@link Configuration} or properties file.
 * <br>
 *
 * @since 0.3.2
 */
public class DAGPropertiesBuilder implements StreamingApplication {

  private static final Logger LOG = LoggerFactory.getLogger(DAGPropertiesBuilder.class);

  public static final String STREAM_PREFIX = "stram.stream";
  public static final String STREAM_SOURCE = "source";
  public static final String STREAM_SINKS = "sinks";
  public static final String STREAM_TEMPLATE = "template";
  public static final String STREAM_LOCALITY = "locality";

  public static final String OPERATOR_PREFIX = "stram.operator.";
  public static final String OPERATOR_CLASSNAME = "classname";
  public static final String OPERATOR_TEMPLATE = "template";

  public static final String TEMPLATE_PREFIX = "stram.template.";

  public static final String TEMPLATE_idRegExp = "matchIdRegExp";
  public static final String TEMPLATE_appNameRegExp = "matchAppNameRegExp";
  public static final String TEMPLATE_classNameRegExp = "matchClassNameRegExp";

  public static final String APPLICATION_PREFIX = "stram.application";
  public static final String APPLICATION_CLASS = "class";

  /**
   * Named set of properties that can be used to instantiate streams or operators
   * with common settings.
   */
  private class TemplateConf {
    private final Properties properties = new Properties();

    /**
     *
     * @param id
     */
    private TemplateConf(String id) {
    }

    private String idRegExp;
    private String appNameRegExp;
    private String classNameRegExp;

  }

  /**
   *
   */
  private class StreamConf {
    private final String id;
    private NodeConf sourceNode;
    private final Set<NodeConf> targetNodes = new HashSet<NodeConf>();

    private final PropertiesWithModifiableDefaults properties = new PropertiesWithModifiableDefaults();
    private TemplateConf template;


    private StreamConf(String id) {
      this.id = id;
    }

    /**
     * Hint to manager that adjacent operators should be deployed in same container.
     * @return boolean
     */
    public DAG.Locality getLocality() {
      String v = properties.getProperty(STREAM_LOCALITY, null);
      return (v != null) ? DAG.Locality.valueOf(v) : null;
    }

    /**
     * Set source on stream to the node output port.
     * @param portName
     * @param node
     */
    public StreamConf setSource(String portName, NodeConf node) {
      if (this.sourceNode != null) {
        throw new IllegalArgumentException(String.format("Stream already receives input from %s", sourceNode));
      }
      node.outputs.put(portName, this);
      this.sourceNode = node;
      return this;
    }

    public StreamConf addSink(String portName, NodeConf targetNode) {
      if (targetNode.inputs.containsKey(portName)) {
        throw new IllegalArgumentException(String.format("Port %s already connected to stream %s", portName, targetNode.inputs.get(portName)));
      }
      //LOG.debug("Adding {} to {}", targetNode, this);
      targetNode.inputs.put(portName, this);
      targetNodes.add(targetNode);
      return this;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", this.id).
          toString();
    }

  }

  /**
   *
   */
  private class PropertiesWithModifiableDefaults extends Properties {
    private static final long serialVersionUID = -4675421720308249982L;

    /**
     * @param defaults
     */
    void setDefaultProperties(Properties defaults) {
        super.defaults = defaults;
    }
  }

  /**
   * Operator configuration
   */
  private class NodeConf {
    public NodeConf(String id) {
      this.id = id;
    }
    private final String id;
    /**
     * The properties of the node, can be subclass properties which will be set via reflection.
     */
    private final PropertiesWithModifiableDefaults properties = new PropertiesWithModifiableDefaults();
    /**
     * The inputs for the node
     */
    private final Map<String, StreamConf> inputs = new HashMap<String, StreamConf>();
    /**
     * The outputs for the node
     */
    private final Map<String, StreamConf> outputs = new HashMap<String, StreamConf>();

    private TemplateConf template;

    /**
     *
     * @return String
     */
    public String getId() {
      return id;
    }

    private String getClassNameReqd() {
      String className = properties.getProperty(OPERATOR_CLASSNAME);
      if (className == null) {
        throw new IllegalArgumentException(String.format("Operator '%s' is missing property '%s'", getId(), DAGPropertiesBuilder.OPERATOR_CLASSNAME));
      }
      return className;
    }

    /**
     * Properties for the node. Template values (if set) become property defaults.
     * @return Map<String, String>
     */
    private Map<String, String> getProperties() {
      return Maps.fromProperties(properties);
    }

    /**
     *
     * @return String
     */
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", this.id).
          toString();
    }

  }

  private final Properties properties = new Properties();
  private final Map<String, NodeConf> nodes;
  private final Map<String, StreamConf> streams;
  private final Map<String, TemplateConf> templates;

  public DAGPropertiesBuilder() {
    this.nodes = new HashMap<String, NodeConf>();
    this.streams = new HashMap<String, StreamConf>();
    this.templates = new HashMap<String,TemplateConf>();
  }

  private NodeConf getOrAddNode(String nodeId) {
    NodeConf nc = nodes.get(nodeId);
    if (nc == null) {
      nc = new NodeConf(nodeId);
      nodes.put(nodeId, nc);
    }
    return nc;
  }

  private StreamConf getOrAddStream(String id) {
    StreamConf sc = streams.get(id);
    if (sc == null) {
      sc = new StreamConf(id);
      streams.put(id, sc);
    }
    return sc;
  }

  private TemplateConf getOrAddTemplate(String id) {
    TemplateConf sc = templates.get(id);
    if (sc == null) {
      sc = new TemplateConf(id);
      templates.put(id, sc);
    }
    return sc;
  }

  /**
   * Add operators from flattened name value pairs in configuration object.
   * @param conf
   */
  public void addFromConfiguration(Configuration conf) {
    addFromProperties(toProperties(conf, "stram."));
  }

  public static Properties toProperties(Configuration conf, String prefix) {
    Iterator<Entry<String, String>> it = conf.iterator();
    Properties props = new Properties();
    while (it.hasNext()) {
      Entry<String, String> e = it.next();
      // filter relevant entries
      if (e.getKey().startsWith(prefix)) {
         props.put(e.getKey(), e.getValue());
      }
    }
    return props;
  }

  private String[] getNodeAndPortId(String s) {
    String[] parts = s.split("\\.");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid node.port reference: " + s);
    }
    return parts;
  }

  /**
   * Read node configurations from properties. The properties can be in any
   * random order, as long as they represent a consistent configuration in their
   * entirety.
   *
   * @param props
   */
  public DAGPropertiesBuilder addFromProperties(Properties props) {

    for (final String propertyName : props.stringPropertyNames()) {
      String propertyValue = props.getProperty(propertyName);
      this.properties.setProperty(propertyName, propertyValue);
      if (propertyName.startsWith(STREAM_PREFIX)) {
         // stream definition
        String[] keyComps = propertyName.split("\\.");
        // must have at least id and single component property
        if (keyComps.length < 4) {
          LOG.warn("Invalid configuration key: {}", propertyName);
          continue;
        }
        String streamId = keyComps[2];
        String propertyKey = keyComps[3];
        StreamConf stream = getOrAddStream(streamId);
        if (STREAM_SOURCE.equals(propertyKey)) {
            if (stream.sourceNode != null) {
              // multiple sources not allowed
              throw new IllegalArgumentException("Duplicate " + propertyName);
            }
            String[] parts = getNodeAndPortId(propertyValue);
            stream.setSource(parts[1], getOrAddNode(parts[0]));
        } else if (STREAM_SINKS.equals(propertyKey)) {
            String[] targetPorts = propertyValue.split(",");
            for (String nodeAndPort : targetPorts) {
              String[] parts = getNodeAndPortId(nodeAndPort.trim());
              stream.addSink(parts[1], getOrAddNode(parts[0]));
            }
        } else if (STREAM_TEMPLATE.equals(propertyKey)) {
          stream.template = getOrAddTemplate(propertyValue);
          // TODO: defer until all keys are read?
          stream.properties.setDefaultProperties(stream.template.properties);
        } else {
           // all other stream properties
          stream.properties.put(propertyKey, propertyValue);
        }
      } else if (propertyName.startsWith(OPERATOR_PREFIX)) {
         // get the node id
         String[] keyComps = propertyName.split("\\.");
         // must have at least id and single component property
         if (keyComps.length < 4) {
           LOG.warn("Invalid configuration key: {}", propertyName);
           continue;
         }
         String nodeId = keyComps[2];
         String propertyKey = keyComps[3];
         NodeConf nc = getOrAddNode(nodeId);
         if (OPERATOR_TEMPLATE.equals(propertyKey)) {
           nc.template = getOrAddTemplate(propertyValue);
           // TODO: defer until all keys are read?
           nc.properties.setDefaultProperties(nc.template.properties);
         } else {
           // simple property
           nc.properties.put(propertyKey, propertyValue);
         }
      } else if (propertyName.startsWith(TEMPLATE_PREFIX)) {
        String[] keyComps = propertyName.split("\\.", 4);
        // must have at least id and single component property
        if (keyComps.length < 4) {
          LOG.warn("Invalid configuration key: {}", propertyName);
          continue;
        }
        TemplateConf tc = getOrAddTemplate(keyComps[2]);
        String propertyKey = keyComps[3];
        if (propertyKey.equals(TEMPLATE_appNameRegExp)) {
          tc.appNameRegExp = propertyValue;
        } else if (propertyKey.equals(TEMPLATE_idRegExp)) {
          tc.idRegExp = propertyValue;
        } else if (propertyKey.equals(TEMPLATE_classNameRegExp)) {
          tc.classNameRegExp = propertyValue;
        } else {
          tc.properties.setProperty(propertyKey, propertyValue);
        }
      }
    }
    return this;
  }

  /**
   * Return all properties set on the builder.
   * Can be serialized to property file and used to read back into builder.
   * @return Properties
   */
  public Properties getProperties() {
    return this.properties;
  }

  @Override
  public void populateDAG(DAG dag, Configuration appConf) {

    Configuration conf = new Configuration(appConf);
    for (final String propertyName : this.properties.stringPropertyNames()) {
      String propertyValue = this.properties.getProperty(propertyName);
      conf.setIfUnset(propertyName, propertyValue);
    }

    Map<NodeConf, Operator> nodeMap = new HashMap<NodeConf, Operator>(this.nodes.size());
    // add all operators first
    for (Map.Entry<String, NodeConf> nodeConfEntry : this.nodes.entrySet()) {
      NodeConf nodeConf = nodeConfEntry.getValue();
      Class<? extends Operator> nodeClass = StramUtils.classForName(nodeConf.getClassNameReqd(), Operator.class);
      Operator nd = dag.addOperator(nodeConfEntry.getKey(), nodeClass);
      setOperatorProperties(nd, nodeConf.getProperties());
      nodeMap.put(nodeConf, nd);
    }

    // wire operators
    for (Map.Entry<String, StreamConf> streamConfEntry : this.streams.entrySet()) {
      StreamConf streamConf = streamConfEntry.getValue();
      DAG.StreamMeta sd = dag.addStream(streamConfEntry.getKey());
      sd.setLocality(streamConf.getLocality());

      if (streamConf.sourceNode != null) {
        String portName = null;
        for (Map.Entry<String, StreamConf> e : streamConf.sourceNode.outputs.entrySet()) {
          if (e.getValue() == streamConf) {
            portName = e.getKey();
          }
        }
        Operator sourceDecl = nodeMap.get(streamConf.sourceNode);
        Operators.PortMappingDescriptor sourcePortMap = new Operators.PortMappingDescriptor();
        Operators.describe(sourceDecl, sourcePortMap);
        sd.setSource(sourcePortMap.outputPorts.get(portName));
      }

      for (NodeConf targetNode : streamConf.targetNodes) {
        String portName = null;
        for (Map.Entry<String, StreamConf> e : targetNode.inputs.entrySet()) {
          if (e.getValue() == streamConf) {
            portName = e.getKey();
          }
        }
        Operator targetDecl = nodeMap.get(targetNode);
        Operators.PortMappingDescriptor targetPortMap = new Operators.PortMappingDescriptor();
        Operators.describe(targetDecl, targetPortMap);
        sd.addSink(targetPortMap.inputPorts.get(portName));
      }
    }

  }

  public static LogicalPlan create(Configuration conf, String tplgPropsFile) throws IOException {
    Properties topologyProperties = readProperties(tplgPropsFile);
    DAGPropertiesBuilder tb = new DAGPropertiesBuilder();
    tb.addFromProperties(topologyProperties);
    LogicalPlan lp = new LogicalPlan();
    tb.populateDAG(lp, conf);
    return lp;
  }

  public static Properties readProperties(String filePath) throws IOException
  {
    InputStream is = new FileInputStream(filePath);
    Properties props = new Properties(System.getProperties());
    props.load(is);
    is.close();
    return props;
  }

  /**
   * Get the configuration properties for the given operator.
   * These can be operator specific settings or settings from matching templates.
   * @param ow
   * @param appName
   */
  public Map<String, String> getProperties(OperatorMeta ow, String appName) {
    // if there are properties set directly, an entry exists
    // else it will be created so we can evaluate the templates against it
    NodeConf n = getOrAddNode(ow.getName());
    n.properties.put(OPERATOR_CLASSNAME, ow.getOperator().getClass().getName());

    Map<String, String> properties = new HashMap<String, String>();
    // list of all templates that match operator, ordered by priority
    if (!this.templates.isEmpty()) {
      TreeMap<Integer, TemplateConf> matchingTemplates = getMatchingTemplates(n, appName);
      if (matchingTemplates != null && !matchingTemplates.isEmpty()) {
        // combined map of prioritized template settings
        for (TemplateConf t : matchingTemplates.descendingMap().values()) {
          properties.putAll(Maps.fromProperties(t.properties));
        }
      }
    }
    // direct settings
    properties.putAll(n.getProperties());
    properties.remove(OPERATOR_CLASSNAME);
    return properties;
  }

  /**
   * Produce the collections of templates that apply for the given id.
   * @param nodeConf
   * @param appName
   * @return TreeMap<Integer, TemplateConf>
   */
  public TreeMap<Integer, TemplateConf> getMatchingTemplates(NodeConf nodeConf, String appName) {
    TreeMap<Integer, TemplateConf> tm = new TreeMap<Integer, TemplateConf>();
    for (TemplateConf t : this.templates.values()) {
      if (t == nodeConf.template) {
        // directly assigned applies last
        tm.put(1, t);
        continue;
      } else if ((t.idRegExp != null && nodeConf.id.matches(t.idRegExp))) {
        tm.put(2, t);
        continue;
      } else if (appName != null && t.appNameRegExp != null
          && appName.matches(t.appNameRegExp)) {
        tm.put(3, t);
        continue;
      } else if (t.classNameRegExp != null
          && nodeConf.getClassNameReqd().matches(t.classNameRegExp)) {
        tm.put(4, t);
        continue;
      }
    }
    return tm;
  }

  /**
   * Inject the configuration properties into the operator instance.
   * @param operator
   * @param properties
   * @return Operator
   */
  public static Operator setOperatorProperties(Operator operator, Map<String, String> properties)
  {
    try {
      // populate custom properties
      BeanUtils.populate(operator, properties);
      return operator;
    }
    catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting operator properties", e);
    }
    catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting operator properties", e);
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getOperatorProperties(Operator operator)
  {
    return new BeanMap(operator);
  }

  /**
   * Set any properties from configuration on the operators in the DAG. This
   * method may throw unchecked exception if the configuration contains
   * properties that are invalid for an operator.
   *
   * @param dag
   */
  public void setOperatorProperties(LogicalPlan dag, String applicationName) {
    for (OperatorMeta ow : dag.getAllOperators()) {
      Map<String, String> properties = getProperties(ow, applicationName);
      setOperatorProperties(ow.getOperator(), properties);
    }
  }

}

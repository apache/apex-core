/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.conf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.malhartech.dag.Node;
import com.malhartech.dag.SerDe;
import com.malhartech.stram.StramUtils;
import com.malhartech.stram.conf.Topology.NodeDecl;
import com.malhartech.stram.conf.Topology.StreamDecl;

/**
 *
 * Builder for the DAG logical representation of nodes and streams<p>
 * <br>
 * Supports reading as name-value pairs from Hadoop Config
 * or programmatic interface.<br>
 * <br>
 *
 */

public class TopologyBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(TopologyBuilder.class);

  private static final String STRAM_DEFAULT_XML_FILE = "stram-default.xml";
  private static final String STRAM_SITE_XML_FILE = "stram-site.xml";


  public static final String STREAM_PREFIX = "stram.stream";
  public static final String STREAM_SOURCE = "source";
  public static final String STREAM_SINKS = "sinks";
  public static final String STREAM_TEMPLATE = "template";
  public static final String STREAM_INLINE = "inline";
  public static final String STREAM_SERDE_CLASSNAME = "serdeClassname";

  public static final String NODE_PREFIX = "stram.node";
  public static final String NODE_CLASSNAME = "classname";
  public static final String NODE_TEMPLATE = "template";

  public static final String NODE_LB_TUPLECOUNT_MIN = "lb.tuplecount.min";
  public static final String NODE_LB_TUPLECOUNT_MAX = "lb.tuplecount.max";

  public static final String TEMPLATE_PREFIX = "stram.template";

  public static Configuration addStramResources(Configuration conf) {
    conf.addResource(STRAM_DEFAULT_XML_FILE);
    conf.addResource(STRAM_SITE_XML_FILE);
    return conf;
  }

  /**
   * Named set of properties that can be used to instantiate streams or nodes
   * with common settings.
   */
  private class TemplateConf {
    private Properties properties = new Properties();

    /**
     *
     * @param id
     */
    private TemplateConf(String id) {
    }
  }

  /**
   *
   */
  public class StreamConf {
    private String id;
    private NodeConf sourceNode;
    private Set<NodeConf> targetNodes = new HashSet<NodeConf>();

    private PropertiesWithModifiableDefaults properties = new PropertiesWithModifiableDefaults();
    private TemplateConf template;


    private StreamConf(String id) {
      this.id = id;
    }

    /**
     *
     * @return String
     */
    public String getId() {
      return id;
    }

    /**
     *
     * @return {com.malhartech.stram.conf.NodeConf}
     */
    public NodeConf getSourceNode() {
      return sourceNode;
    }

    /**
     *
     * @return {com.malhartech.stram.conf.NodeConf}
     */
    public Set<NodeConf> getTargetNodes() {
      return targetNodes;
    }

    /**
     *
     * @param key get property of key
     * @return String
     */
    public String getProperty(String key) {
      return properties.getProperty(key);
    }

    /**
     * Immutable properties. Template values (if set) become defaults.
     * @return Map<String, String>
     */
    public Map<String, String> getProperties() {
      return Maps.fromProperties(properties);
    }

    /**
     *
     * @param key
     * @param value
     */
    public StreamConf addProperty(String key, String value) {
      properties.put(key, value);
      return this;
    }

    /**
     * Hint to manager that adjacent nodes should be deployed in same container.
     * @return boolean
     */
    public boolean isInline() {
      return Boolean.TRUE.toString().equals(properties.getProperty(STREAM_INLINE, Boolean.FALSE.toString()));
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
      rootNodes.removeAll(this.targetNodes);
      return this;
    }

    public StreamConf addSink(String portName, NodeConf targetNode) {
      if (targetNode.inputs.containsKey(portName)) {
        throw new IllegalArgumentException(String.format("Port %s already connected to stream %s", portName, targetNode.inputs.get(portName)));
      }
      //LOG.debug("Adding {} to {}", targetNode, this);
      targetNode.inputs.put(portName, this);
      targetNodes.add(targetNode);
      // root nodes don't receive input from other node(s)
      if (sourceNode != null) {
        rootNodes.remove(targetNode);
      }
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
     * Hint to manager that adjacent nodes should be deployed in same container.
     * @param defaults
     */
    void setDefaultProperties(Properties defaults) {
        super.defaults = defaults;
    }
  }

  /**
   * Node configuration
   */
  public class NodeConf {
    public NodeConf(String id) {
      this.id = id;
    }
    private String id;
    /**
     * The properties of the node, can be subclass properties which will be set via reflection.
     */
    private PropertiesWithModifiableDefaults properties = new PropertiesWithModifiableDefaults();
    /**
     * The inputs for the node
     */
    Map<String, StreamConf> inputs = new HashMap<String, StreamConf>();
    /**
     * The outputs for the node
     */
    Map<String, StreamConf> outputs = new HashMap<String, StreamConf>();

    private TemplateConf template;

    /**
     *
     * @return String
     */
    public String getId() {
      return id;
    }

    public String getNodeClassNameReqd() {
      String className = properties.getProperty(NODE_CLASSNAME);
      if (className == null) {
        throw new IllegalArgumentException(String.format("Configuration for node '%s' is missing property '%s'", getId(), TopologyBuilder.NODE_CLASSNAME));
      }
      return className;
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

    /**
     *
     * @param portName
     * @return StreamConf
     */
    public StreamConf getInput(String portName) {
      return inputs.get(portName);
    }

    /**
     *
     * @return Collection<StreamConf>
     */
    public Collection<StreamConf> getInputStreams() {
      return inputs.values();
    }

    public StreamConf getOutput(String portName) {
      return outputs.get(portName);
    }

    public Collection<StreamConf> getOutputStreams() {
      return outputs.values();
    }


    public void setClassName(String className) {
      this.properties.put(NODE_CLASSNAME, className);
    }

    public void setProperty(String name, String value) {
      this.properties.put(name, value);
    }

    /**
     * Properties for the node. Template values (if set) become property defaults.
     * @return Map<String, String>
     */
    public Map<String, String> getProperties() {
      return Maps.fromProperties(properties);
    }

  }

  private final Configuration conf = new Configuration(false);
  private final Map<String, NodeConf> nodes;
  private final Map<String, StreamConf> streams;
  private final Map<String, TemplateConf> templates;
  private final Set<NodeConf> rootNodes; // root nodes (nodes that don't have input from another node)

  public TopologyBuilder() {
    this.nodes = new HashMap<String, NodeConf>();
    this.streams = new HashMap<String, StreamConf>();
    this.templates = new HashMap<String,TemplateConf>();
    this.rootNodes = new HashSet<NodeConf>();
  }

  /**
   * Create topology from given configuration.
   * More nodes can be added programmatically.
   * @param conf
   */
  public TopologyBuilder(Configuration conf) {
    this();
    addFromConfiguration(conf);
  }


  private NodeConf getOrAddNode(String nodeId) {
    NodeConf nc = nodes.get(nodeId);
    if (nc == null) {
      nc = new NodeConf(nodeId);
      nodes.put(nodeId, nc);
      rootNodes.add(nc);
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
   * Add nodes from flattened name value pairs in configuration object.
   * @param conf
   */
  public void addFromConfiguration(Configuration conf) {
    addFromProperties(toProperties(conf));
  }

  public static Properties toProperties(Configuration conf) {
    Iterator<Entry<String, String>> it = conf.iterator();
    Properties props = new Properties();
    while (it.hasNext()) {
      Entry<String, String> e = it.next();
      // filter relevant entries
      if (e.getKey().startsWith("stram.")) {
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
  public TopologyBuilder addFromProperties(Properties props) {

    for (final String propertyName : props.stringPropertyNames()) {
      String propertyValue = props.getProperty(propertyName);
      conf.set(propertyName, propertyValue);
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
      } else if (propertyName.startsWith(NODE_PREFIX)) {
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
         if (NODE_TEMPLATE.equals(propertyKey)) {
           nc.template = getOrAddTemplate(propertyValue);
           // TODO: defer until all keys are read?
           nc.properties.setDefaultProperties(nc.template.properties);
         } else {
           // simple property
           nc.properties.put(propertyKey, propertyValue);
         }
      } else if (propertyName.startsWith(TEMPLATE_PREFIX)) {
        String[] keyComps = propertyName.split("\\.");
        // must have at least id and single component property
        if (keyComps.length < 4) {
          LOG.warn("Invalid configuration key: {}", propertyName);
          continue;
        }
        String propertyKey = keyComps[3];
        TemplateConf tc = getOrAddTemplate(keyComps[2]);
        tc.properties.setProperty(propertyKey, propertyValue);
      }
    }
    return this;
  }

  /**
   * Map of fully constructed node configurations with inputs/outputs set.
   * @return Map<String, NodeConf>
   */
  public Map<String, NodeConf> getAllNodes() {
    return Collections.unmodifiableMap(this.nodes);
  }

  /**
   * @return Set<NodeConf>
   */
  public Set<NodeConf> getRootNodes() {
    return Collections.unmodifiableSet(this.rootNodes);
  }

  /**
   * Return all properties of the topology constructed by the builder.
   * Can be serialized to property file and used to read back into builder.
   * @return Properties
   */
  public Properties getProperties() {
    return TopologyBuilder.toProperties(this.conf);
  }

  /**
   *
   * @return Topology
   */
  public Topology getTopology() {

    Topology tplg = new Topology(conf);

    Map<NodeConf, NodeDecl> nodeMap = new HashMap<NodeConf, NodeDecl>(this.nodes.size());
    // add all nodes first
    for (Map.Entry<String, NodeConf> nodeConfEntry : this.nodes.entrySet()) {
      NodeConf nodeConf = nodeConfEntry.getValue();
      Class<? extends Node> nodeClass = StramUtils.classForName(nodeConf.getNodeClassNameReqd(), Node.class);
      NodeDecl nd = tplg.addNode(nodeConfEntry.getKey(), nodeClass);
      nd.getProperties().putAll(nodeConf.getProperties());
      nodeMap.put(nodeConf, nd);
    }

    // wire nodes
    for (Map.Entry<String, StreamConf> streamConfEntry : this.streams.entrySet()) {
      StreamConf streamConf = streamConfEntry.getValue();
      StreamDecl sd = tplg.addStream(streamConfEntry.getKey());
      sd.setInline(streamConf.isInline());

      String serdeClassName = streamConf.getProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME);
      if (serdeClassName != null) {
        sd.setSerDeClass(StramUtils.classForName(serdeClassName, SerDe.class));
      }

      if (streamConf.sourceNode != null) {
        String portName = null;
        for (Map.Entry<String, StreamConf> e : streamConf.sourceNode.outputs.entrySet()) {
          if (e.getValue() == streamConf) {
            portName = e.getKey();
          }
        }
        NodeDecl sourceDecl = nodeMap.get(streamConf.sourceNode);
        sd.setSource(sourceDecl.getOutput(portName));
      }

      for (NodeConf targetNode : streamConf.targetNodes) {
        String portName = null;
        for (Map.Entry<String, StreamConf> e : targetNode.inputs.entrySet()) {
          if (e.getValue() == streamConf) {
            portName = e.getKey();
          }
        }
        NodeDecl targetDecl = nodeMap.get(targetNode);
        sd.addSink(targetDecl.getInput(portName));
      }
    }

    return tplg;
  }


  public static Topology createTopology(Configuration conf, String tplgPropsFile) throws IOException {
    Properties topologyProperties = readProperties(tplgPropsFile);
    TopologyBuilder tb = new TopologyBuilder(conf);
    tb.addFromProperties(topologyProperties);
    return tb.getTopology();
  }

  public static Properties readProperties(String filePath) throws IOException
  {
    InputStream is = new FileInputStream(filePath);
    Properties props = new Properties(System.getProperties());
    props.load(is);
    is.close();
    return props;
  }

}

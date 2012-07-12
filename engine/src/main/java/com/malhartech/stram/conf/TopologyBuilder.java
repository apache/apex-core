package com.malhartech.stram.conf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.malhartech.dag.StreamConfiguration;

/**
 * Builder for the DAG logical representation of nodes and streams.
 * Supports reading as name-value pairs from Hadoop Configuration
 * or programmatic interface. 
 */
public class TopologyBuilder {
  
  private static Logger LOG = LoggerFactory.getLogger(TopologyBuilder.class);
  
  private static final String STRAM_DEFAULT_XML_FILE = "stram-default.xml";
  private static final String STRAM_SITE_XML_FILE = "stram-site.xml";

  /**
   * Comma separated list of jar files that will be made available to stram app master and child containers
   */
  public static final String LIBJARS = "stram.libjars";
  
  public static final String STREAM_PREFIX = "stram.stream";
  public static final String STREAM_SOURCENODE = "inputNode";
  public static final String STREAM_TARGETNODE = "outputNode";
  public static final String STREAM_TEMPLATE = "template";
  public static final String STREAM_INLINE = "inline";
  
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
  public class TemplateConf {
    private String id;
    private Properties properties = new Properties();
    
    private TemplateConf(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public void addProperty(String key, String value) {
      properties.setProperty(key, value);
    }
  }
  
  
  public class StreamConf {
    private String id;
    private NodeConf sourceNode;
    private NodeConf targetNode;
    
    private PropertiesWithModifiableDefaults properties = new PropertiesWithModifiableDefaults();
    private TemplateConf template;

    
    private StreamConf(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public NodeConf getSourceNode() {
      return sourceNode;
    }

    public NodeConf getTargetNode() {
      return targetNode;
    }

    public String getProperty(String key) {
      return properties.getProperty(key);
    }

    /**
     * Immutable properties. Template values (if set) become defaults. 
     * @return
     */
    public Map<String, String> getProperties() {
      return Maps.fromProperties(properties);
    }
    
    public void addProperty(String key, String value) {
      properties.put(key, value);
    }
    
    /**
     * Hint to manager that adjacent nodes should be deployed in same container.
     * @return
     */
    public boolean isInline() {
      return Boolean.TRUE.toString().equals(properties.getProperty(STREAM_INLINE, Boolean.FALSE.toString()));
    }
    
  }

  class PropertiesWithModifiableDefaults extends Properties {
    private static final long serialVersionUID = -4675421720308249982L;

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
    
    private Integer nindex; // for cycle detection
    private Integer lowlink; // for cycle detection   

    public String getId() {
      return id;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", this.id).
          toString();
    }

    public NodeConf addInput(StreamConf stream) {
      if (stream.targetNode != null) {
        // multiple targets not allowed
        throw new IllegalArgumentException("Stream already connected to target " + stream.targetNode);
      }
      inputs.put(stream.id, stream);
      stream.targetNode = this;
      if (stream.sourceNode != null) {
        // root nodes don't receive input from another node
        rootNodes.remove(stream.targetNode);
      }
      return this;
    }

    public StreamConf getInput(String streamId) {
      return inputs.get(streamId);
    }

    public Collection<StreamConf> getInputStreams() {
      return inputs.values();
    }
    
    public NodeConf addOutput(StreamConf stream) {
      if (stream.sourceNode != null) {
        // multiple targets not allowed
        throw new IllegalArgumentException("Stream already connected to source " + stream.sourceNode);
      }
      outputs.put(stream.id, stream);
      stream.sourceNode = this;
      if (stream.targetNode != null) {
        // root nodes don't receive input from another node
        rootNodes.remove(stream.targetNode);
      }
      return this;
    }
    
    public StreamConf getOutput(String streamId) {
      return outputs.get(streamId);
    }
    
    public Collection<StreamConf> getOutputStreams() {
      return outputs.values();
    }
    
    
    public void setClassName(String className) {
      this.properties.put(NODE_CLASSNAME, className);
    }
    
    /**
     * Properties for the node. Template values (if set) become property defaults. 
     * @return
     */
    public Map<String, String> getProperties() {
      return Maps.fromProperties(properties);
    }
    
  }

  final private Map<String, NodeConf> nodes;
  final private Map<String, StreamConf> streams;
  final private Map<String, TemplateConf> templates;
  final private Set<NodeConf> rootNodes; // root nodes (nodes that don't have input from another node)
  private int nodeIndex = 0; // used for cycle validation
  private Stack<NodeConf> stack = new Stack<NodeConf>(); // used for cycle validation
  
  /**
   * Create topology from given configuration. 
   * More nodes can be added programmatically.
   * @param conf
   */
  public TopologyBuilder(Configuration conf) {
    this.nodes = new HashMap<String, NodeConf>();
    this.streams = new HashMap<String, StreamConf>();
    this.templates = new HashMap<String,TemplateConf>();
    this.rootNodes = new HashSet<NodeConf>();
    addFromConfiguration(conf);
  }
  
  public NodeConf getOrAddNode(String nodeId) {
    NodeConf nc = nodes.get(nodeId);
    if (nc == null) {
      nc = new NodeConf(nodeId);
      nodes.put(nodeId, nc);
      rootNodes.add(nc);
    }
    return nc;
  }

  public StreamConf getOrAddStream(String id) {
    StreamConf sc = streams.get(id);
    if (sc == null) {
      sc = new StreamConf(id);
      streams.put(id, sc);
    }
    return sc;
  }
  
  public TemplateConf getOrAddTemplate(String id) {
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
    // turn relevant entries into properties
    Iterator<Entry<String, String>> it = conf.iterator();
    Properties props = new Properties();
    while (it.hasNext()) {
      Entry<String, String> e = it.next();
      if (e.getKey().startsWith("stram.")) {
         props.put(e.getKey(), e.getValue());
      }
    }
    addFromProperties(props);   
  }
  
  /**
   * Read node configurations from properties. The properties can be in any
   * random order, as long as they represent a consistent configuration in their
   * entirety.
   * 
   * @param props
   * @return
   */
  public void addFromProperties(Properties props) {
    
    for (final String propertyName : props.stringPropertyNames()) {
      String propertyValue = props.getProperty(propertyName);
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
        if (STREAM_SOURCENODE.equals(propertyKey)) {
            if (stream.sourceNode != null) {
              // multiple sources not allowed
              throw new IllegalArgumentException("Duplicate " + propertyName);
            }
            getOrAddNode(propertyValue).addOutput(stream);
        } else if (STREAM_TARGETNODE.equals(propertyKey)) {
          if (stream.targetNode != null) {
              // multiple targets not allowed
              throw new IllegalArgumentException("Duplicate " + propertyName);
            }
            getOrAddNode(propertyValue).addInput(stream);
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
  }
  
  /**
   * Map of fully constructed node configurations with inputs/outputs set.
   * @return
   */
  public Map<String, NodeConf> getAllNodes() {
    return Collections.unmodifiableMap(this.nodes);
  }

  public Set<NodeConf> getRootNodes() {
    return Collections.unmodifiableSet(this.rootNodes);
  }

  /**
   * Check for cycles in the graph reachable from start node n.
   * This is done by attempting to find a strongly connected components,
   * see http://en.wikipedia.org/wiki/Tarjan%E2%80%99s_strongly_connected_components_algorithm
   * @param n
   */
  public void findStronglyConnected(NodeConf n, List<List<String>> cycles) {
    n.nindex = nodeIndex;
    n.lowlink = nodeIndex;
    nodeIndex++;
    stack.push(n);

    // depth first successors traversal
    for (StreamConf downStream : n.outputs.values()) {
       NodeConf successor = downStream.targetNode;
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

    // pop stack for all root nodes    
    if (n.lowlink.equals(n.nindex)) {
       List<String> connectedIds = new ArrayList<String>();
       while (!stack.isEmpty()) {
         NodeConf n2 = stack.pop();
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

  public void validate() {   
    // clear visited on all nodes
    for (NodeConf n : nodes.values()) {
      n.nindex = null;
      n.lowlink = null;
    }
    
    List<List<String>> cycles = new ArrayList<List<String>>();
    for (NodeConf n : nodes.values()) {
      if (n.nindex == null) {
        findStronglyConnected(n, cycles);
      }
    }
    if (!cycles.isEmpty()) {
      throw new IllegalStateException("Loops detected in the graph: " + cycles);
    }
    
    for (StreamConf s : streams.values()) {
      if (s.getSourceNode() == null && s.getTargetNode() == null) {
        throw new IllegalStateException(String.format("Source or target needs to be defined for stream %s", s.getId()));
      } else if (s.getSourceNode() == null || s.getTargetNode() == null) {
        if (s.getProperty(StreamConfiguration.STREAM_CLASSNAME) == null) {
          throw new IllegalStateException(String.format("Property %s needs to be defined for adapter stream %s", StreamConfiguration.STREAM_CLASSNAME, s.getId()));
        }
      }
    }
    
  }

  /**
   * Configured class dependencies for the topology. Used to determine jar file dependencies. 
   * @return
   */
  public Set<String> getClassNames() {
    Set<String> classNames = new HashSet<String>();
    for (NodeConf n : this.nodes.values()) {
      String className = n.properties.getProperty(NODE_CLASSNAME);
      if (className != null) {
        classNames.add(className);
      }
    }
    for (StreamConf n : this.streams.values()) {
      String className = n.properties.getProperty(StreamConfiguration.STREAM_CLASSNAME);
      if (className != null) {
        classNames.add(className);
      }
    }
    return classNames;
  }
  
  
}

package com.malhar.stram.conf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for the DAG logical representation of nodes and streams.
 * Supports reading as name-value pairs from Hadoop Configuration
 * or programmatic interface. 
 */
public class TopologyBuilder {
  
  private static Logger LOG = LoggerFactory.getLogger(TopologyBuilder.class);
  
  private static final String STRAM_DEFAULT_XML_FILE = "stram-default.xml";
  private static final String STRAM_SITE_XML_FILE = "stram-site.xml";

  public static final String ROOT_NODE_ID = "stram.rootnode";
  public static final String NODE_PREFIX = "stram.node.no";
  
  public static final String NODE_CLASSNAME = "classname";
  public static final String NODE_INPUT = "input";
  public static final String NODE_INPUT_NAME = "name";
  
  
  public static Configuration addStramResources(Configuration conf) {
    conf.addResource(STRAM_DEFAULT_XML_FILE);
    conf.addResource(STRAM_SITE_XML_FILE);
    return conf;
  }

  public class StreamConf {
    //private String sourceNodeId;
    private String name;
    private Map<String, String> properties = new HashMap<String, String>();
    
    private StreamConf(NodeConf inputNode, NodeConf outputNode) {
      //this.sourceNodeId = sourceNodeId;
    }

    public String getName() {
      return name;
    }
    
    public String getProperty(String key) {
      return properties.get(key);
    }
    
    public void addProperty(String key, String value) {
      properties.put(key, value);
    }
  }
  
  /**
   * DNode configuration, serializable to node container 
   */
  public class NodeConf {
    public NodeConf(String id) {
      this.id = id;
    }
    String id;
    /**
     * The properties of the node, can be subclass properties which will be set via reflection.
     */
    Map<String, String> properties = new HashMap<String, String>();
    /**
     * The inputs for the node
     */
    Map<NodeConf, StreamConf> inputs = new HashMap<NodeConf, StreamConf>();
    /**
     * The inputs for the node
     */
    Map<NodeConf, StreamConf> outputs = new HashMap<NodeConf, StreamConf>();

    private Integer nindex; // for cycle detection
    private Integer lowlink; // for cycle detection   
    
    /**
     * Declare another node as upstream
     * Currently only used for topology unit testing, to be included into topology builder API later
     * @param inputNode
     */
    public StreamConf addInput(NodeConf inputNode) {
      if (nodeConfs.get(inputNode.id) != inputNode) {
        // attempt to link node from another instance?
        throw new IllegalArgumentException("Can only link nodes within one topology: " + inputNode);
      }
      StreamConf sconf = inputs.get(inputNode);
      if (sconf == null) {
        sconf = new StreamConf(inputNode, this);
        inputs.put(inputNode, sconf);
        rootNodes.remove(this);
      }
      // add reverse output link
      inputNode.outputs.put(this, sconf);
      return sconf;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", this.id).
          toString();
    }
    
  }

  private Map<String, NodeConf> nodeConfs;
  private Set<NodeConf> rootNodes; // root nodes (nodes that don't have input from another node)
  private Configuration conf;
  private int nodeIndex = 0; // used for cycle validation
  private Stack<NodeConf> stack = new Stack<NodeConf>(); // used for cycle validation
  
  /**
   * Create topology from given configuration. 
   * More nodes can be added programmatically.
   * @param conf
   */
  public TopologyBuilder(Configuration conf) {
    this.conf = conf;
    this.nodeConfs = new HashMap<String, NodeConf>();
    this.rootNodes = new HashSet<NodeConf>();
    readNodeConfig();
  }
  
  public NodeConf getOrAddNode(String nodeId) {
    NodeConf nc = nodeConfs.get(nodeId);
    if (nc == null) {
      nc = new NodeConf(nodeId);
      nodeConfs.put(nodeId, nc);
      rootNodes.add(nc);
    }
    return nc;
  }
  
  /**
   * Read all node configurations from the configuration object.
   * @param conf
   * @return
   */
  private void readNodeConfig() {
    
    Iterator<Entry<String, String>> it = conf.iterator();
    while (it.hasNext()) {
      Entry<String, String> e = it.next();
      if (e.getKey().startsWith(NODE_PREFIX)) {
         // get the node id
         String[] keyComps = e.getKey().split("\\.");
         // must have at least id and single component property
         if (keyComps.length < 4) {
           LOG.warn("Invalid configuration key: {}", e.getKey());
           continue;
         }
         String nodeId = keyComps[2];
         String propertyKey = keyComps[3];
         NodeConf nc = getOrAddNode(nodeId);

         // distinguish input declarations and other properties
         if (NODE_INPUT.equals(propertyKey)) {
            if (keyComps.length == 4) {
                // list of input nodes
                String[] nodeIds = StringUtils.splitByWholeSeparator(e.getValue(), ",");
                // merge to input containers
                for (String inputNodeId : nodeIds) {
                    nc.addInput(getOrAddNode(inputNodeId));
                }
            } else {
                // get or add the input
                NodeConf inputNode = getOrAddNode(keyComps[4]);
                StreamConf sconf = nc.addInput(inputNode);
                if (keyComps.length > 5) {
                  if (keyComps[5].equals(NODE_INPUT_NAME)) {
                      sconf.name = e.getValue();
                  } else {
                    // next key component is interpreted as input property key
                    sconf.addProperty(keyComps[5], e.getValue());
                  }
                }
            }
         } else {
           // simple property
           nc.properties.put(propertyKey, e.getValue());
         }
      }
    }
  }
  
  /**
   * Map of fully constructed node configurations with inputs/outputs set.
   * @return
   */
  public Map<String, NodeConf> getAllNodes() {
    return Collections.unmodifiableMap(this.nodeConfs);
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
    for (NodeConf successor : n.outputs.keySet()) {
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
       // cycle if more than one node in stack       
       if (connectedIds.size() > 1 || n.outputs.containsKey(n)) {
         LOG.debug("detected cycle from node {}: {}", n.id, connectedIds);
         cycles.add(connectedIds);
       }
    }
  }

  public void validate() {   
    // clear visited on all nodes
    for (NodeConf n : nodeConfs.values()) {
      n.nindex = null;
      n.lowlink = null;
    }
    
    List<List<String>> cycles = new ArrayList<List<String>>();
    for (NodeConf n : nodeConfs.values()) {
      if (n.nindex == null) {
        findStronglyConnected(n, cycles);
      }
    }
    if (!cycles.isEmpty()) {
      throw new IllegalStateException("Loops detected in the graph: " + cycles);
    }
  }
    
}

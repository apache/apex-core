/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.SerDe;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Derives the physical model from the logical dag and assigned to hadoop container. Is the initial query planner<p>
 * <br>
 * Does the static binding of dag to physical nodes. Parse the dag and figures out the topology. The upstream
 * dependencies are deployed first. Static partitions are defined by the dag are enforced. Stram an later on do
 * dynamic optimization.<br>
 * In current implementation optimization is not done with number of containers. The number provided in the dag
 * specification is treated as minimum as well as maximum. Once the optimization layer is built this would change<br>
 * Topology deployment thus blocks successful running of a streaming job in the current version of the streaming platform<br>
 * <br>
 */
public class TopologyDeployer {

  private final static Logger LOG = LoggerFactory.getLogger(TopologyDeployer.class);

  /**
   * Common abstraction for streams and nodes for heartbeat/monitoring.<p>
   * <br>
   *
   */
  public abstract static class PTComponent {
    String id;
    
    /**
     * 
     * @return String
     */
    abstract public String getLogicalId();
    // stats

    /**
     * 
     * @return String
     */
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", id).
          append("logicalId", getLogicalId()).
          toString();
    }

  }

  /**
   *
   * Representation of an input in the physical layout. A source in the DAG<p>
   * <br>
   * This can come from another node or from outside the DAG<br>
   * <br>
   *
   */
  public static class PTInput extends PTComponent {
    final TopologyBuilder.StreamConf logicalStream;
    final PTComponent target;
    final byte[] partition;
    final PTComponent source;

    /**
     * 
     * @param logicalStream
     * @param target
     * @param partition
     * @param source 
     */
    protected PTInput(StreamConf logicalStream, PTComponent target, byte[] partition, PTComponent source) {
      this.logicalStream = logicalStream;
      this.target = target;
      this.partition = partition;
      this.source = source;
    }

    /**
     * 
     * @return String
     */
    @Override
    public String getLogicalId() {
      return logicalStream.getId();
    }
    
    /**
     * 
     * @return InetSocketAddress
     */
    public InetSocketAddress getBufferServerAddress() {
      if (source instanceof PTNode) {
        return ((PTNode)source).container.bufferServerAddress;
      } else {
        return ((PTNode)target).container.bufferServerAddress;
      }
    }

  }

  /**
   *
   * Representation of input adapter in the physical layout<p>
   * <br>
   *
   */
  public static class PTInputAdapter extends PTInput {
      /**
       * 
       * @param logicalStream
       * @param target
       * @param partition 
       */
    protected PTInputAdapter(StreamConf logicalStream, PTComponent target, byte[] partition) {
      super(logicalStream, target, partition, null);
    }
  }

  /**
   *
   * Representation of an output in the physical layout. A sink in the DAG<p>
   * <br>
   * This can go to another node or to a output Adapter (i.e. outside the DAG)<br>
   * <br>
   *
   */
  public static class PTOutput extends PTComponent {
    final TopologyBuilder.StreamConf logicalStream;
    final PTComponent source;
    
    /**
     * Constructor
     * @param logicalStream
     * @param source 
     */
    protected PTOutput(StreamConf logicalStream, PTComponent source) {
      this.logicalStream = logicalStream;
      this.source = source;
    }

    /**
     * 
     * @return String
     */
    @Override
    public String getLogicalId() {
      return logicalStream.getId();
    }

  }

  /**
   *
   * Representation of output adapter in the physical layout<p>
   * <br>
   *
   */
  public static class PTOutputAdapter extends PTOutput {
      /**
       * 
       * @param logicalStream
       * @param source 
       */
    protected PTOutputAdapter(StreamConf logicalStream, PTComponent source) {
      super(logicalStream, source);
    }
  }

  /**
   *
   * Representation of a node in the physical layout<p>
   * <br>
   * A generic node in the DAG<br>
   * <br>
   *
   */
  public static class PTNode extends PTComponent {
    TopologyBuilder.NodeConf logicalNode;
    List<PTInput> inputs;
    List<PTOutput> outputs;
    PTContainer container;
    LinkedList<Long> checkpointWindows = new LinkedList<Long>();
    
    /**
     * 
     * @return {@link com.malhartech.stram.conf.NodeConf}
     */
    public NodeConf getLogicalNode() {
      return this.logicalNode;
    }

    /**
     * 
     * @return long
     */
    public long getRecentCheckpoint() {
      if (checkpointWindows != null && !checkpointWindows.isEmpty())
        return checkpointWindows.getLast();
      return 0;
    }
    
    /**
     * 
     * @return String
     */
    @Override
    public String getLogicalId() {
      return logicalNode.getId();
    }

  }

  /**
   *
   * Representation of a container for physical objects of dag to be placed in<p>
   * <br>
   * This class directly maps to a hadoop container<br>
   * <br>
   *
   */

  public static class PTContainer {
    List<PTNode> nodes = new ArrayList<PTNode>();
    String containerId; // assigned to yarn container
    InetSocketAddress bufferServerAddress;

    /**
     * 
     * @return String
     */
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("nodes", this.nodes).
          toString();
    }
  }

  private Map<NodeConf, List<PTNode>> deployedNodes = new HashMap<NodeConf, List<PTNode>>();
  private Map<StreamConf, PTOutputAdapter> outputAdapters = new HashMap<StreamConf, PTOutputAdapter>();
  private Map<StreamConf, PTInputAdapter> inputAdapters = new HashMap<StreamConf, PTInputAdapter>();
  private List<PTContainer> containers = new ArrayList<PTContainer>();
  private int maxContainers = 1;

  private PTContainer getContainer(int index) {
    if (index >= containers.size()) {
      if (index >= maxContainers) {
        index = maxContainers - 1;
      }
      for (int i=containers.size(); i<index+1; i++) {
        containers.add(i, new PTContainer());
      }
    }
    return containers.get(index);
  }
  
  /**
   * 
   * @param maxContainers
   * @param tb 
   */
  public void init(int maxContainers, TopologyBuilder tb) {

    this.maxContainers = Math.max(maxContainers,1);
    Stack<NodeConf> pendingNodes = new Stack<NodeConf>();
    for (NodeConf n : tb.getAllNodes().values()) {
      pendingNodes.push(n);
    }

    int nodeCount = 0;

    while (!pendingNodes.isEmpty()) {
      NodeConf n = pendingNodes.pop();

      if (deployedNodes.containsKey(n)) {
        // node already deployed as upstream dependency
        continue;
      }

      // look at all input streams to determine number of nodes
      // and upstream dependencies
      byte[][] partitions = null;
      boolean upstreamDeployed = true;
      PTNode inlineUpstreamNode = null;

      for (StreamConf s : n.getInputStreams()) {
        if (s.getSourceNode() != null && !deployedNodes.containsKey(s.getSourceNode())) {
          pendingNodes.push(n);
          pendingNodes.push(s.getSourceNode());
          upstreamDeployed = false;
          break;
        }
        byte[][] streamPartitions = getStreamPartitions(s);
        if (streamPartitions != null) {
          if (partitions != null) {
            if (!Arrays.deepEquals(partitions, streamPartitions)) {
              throw new IllegalArgumentException("Node cannot have multiple input streams with different partitions.");
            }
          }
          partitions = streamPartitions;
        } else {
          if (s.isInline()) {
            // node to be deployed with source node
            if (s.getSourceNode() != null) {
              // find the container for the node?
              List<PTNode> deployedNodes = this.deployedNodes.get(s.getSourceNode());
              inlineUpstreamNode = deployedNodes.get(0);
            }
          }
        }
      }

      if (upstreamDeployed) {
        // ready to deploy this node
        List<PTNode> pnodes = new ArrayList<PTNode>();
        if (partitions != null) {
          // create node per partition,
          // distribute over available containers
          for (int i = 0; i < partitions.length; i++) {
            PTNode pNode = createPTNode(n, partitions[i], pnodes.size());
            pnodes.add(pNode);
            PTContainer container = getContainer((nodeCount++) % maxContainers);
            container.nodes.add(pNode);
            pNode.container = container;
          }
        } else {
          // single instance, no partitions
          PTNode pNode = createPTNode(n, null, pnodes.size());
          pnodes.add(pNode);

          PTContainer container;
          if (inlineUpstreamNode != null) {
            container = inlineUpstreamNode.container;
          } else {
            container = getContainer((nodeCount++) % maxContainers);
          }
          container.nodes.add(pNode);
          pNode.container = container;
        }
        this.deployedNodes.put(n, pnodes);
      }
    }

  }

  private AtomicInteger nodeSequence = new AtomicInteger();

  private PTNode createPTNode(NodeConf nodeConf, byte[] partition, int instanceCount) {

    PTNode pNode = new PTNode();
    pNode.logicalNode = nodeConf;
    pNode.inputs = new ArrayList<PTInput>();
    pNode.outputs = new ArrayList<PTOutput>();
    pNode.id = ""+nodeSequence.incrementAndGet();

    for (StreamConf inputStream : nodeConf.getInputStreams()) {
      // find upstream node(s),
      // (can be multiple with partitioning or load balancing)
      if (inputStream.getSourceNode() != null) {
        List<PTNode> upstreamNodes = deployedNodes.get(inputStream.getSourceNode());
        for (PTNode upNode : upstreamNodes) {
          // link to upstream output(s) for this stream
          for (PTOutput upstreamOut : upNode.outputs) {
            if (upstreamOut.logicalStream == inputStream) {
              PTInput input = new PTInput(inputStream, pNode, partition, upNode);
              pNode.inputs.add(input);
            }
          }
        }
      } else {
        // input adapter
        if (instanceCount == 0) {
          // create adapter wrapper node
          PTInputAdapter adapter = new PTInputAdapter(inputStream, pNode, partition);
          adapter.id = ""+nodeSequence.incrementAndGet();
          pNode.inputs.add(adapter);
          inputAdapters.put(inputStream, adapter);
        } else {
          // stream from adapter wrapper node
          PTInputAdapter adapter = inputAdapters.get(inputStream);
          PTInput input = new PTInput(inputStream, pNode, partition, adapter);
          pNode.inputs.add(input);
        }
      }
    }

    for (StreamConf outputStream : nodeConf.getOutputStreams()) {
      if (outputStream.getTargetNode() != null) {
        pNode.outputs.add(new PTOutput(outputStream, pNode));
      } else {
        // output adapter
        if (instanceCount == 0) {
          // create single adapter wrapper node
          PTOutputAdapter adapter = new PTOutputAdapter(outputStream, pNode);
          adapter.id = ""+nodeSequence.incrementAndGet();
          pNode.outputs.add(adapter);
          outputAdapters.put(outputStream, adapter);
        } else {
          // stream to adapter wrapper node
          PTOutput output = new PTOutput(outputStream, pNode);
          pNode.outputs.add(output);
        }
      }
    }

    return pNode;
  }

  private byte[][] getStreamPartitions(StreamConf streamConf)
  {
    try {
      SerDe serde = StramUtils.getSerdeInstance(streamConf.getProperties());
      byte[][] partitions = serde.getPartitions();
      if (partitions != null) {
        //return new ArrayList<byte[]>(Arrays.asList(serde.getPartitions()));
        return partitions;
      }
    }
    catch (Exception e) {
      LOG.error("Failed to get partition info from SerDe", e);
    }
    return null;
  }


  protected List<PTContainer> getContainers() {
    return this.containers;
  }

  protected List<PTNode> getNodes(NodeConf nodeConf) {
    return this.deployedNodes.get(nodeConf);
  }

  protected PTInputAdapter getInputAdapter(StreamConf streamConf) {
    return this.inputAdapters.get(streamConf);
  }

}

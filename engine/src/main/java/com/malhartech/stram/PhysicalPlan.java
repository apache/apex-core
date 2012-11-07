/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.DAG;
import com.malhartech.api.DAG.OperatorWrapper;
import com.malhartech.dag.SerDe;
import com.malhartech.api.DAG.StreamDecl;

/**
 *
 * Derives the physical model from the logical dag and assigned to hadoop container. Is the initial query planner<p>
 * <br>
 * Does the static binding of dag to physical operators. Parse the dag and figures out the topology. The upstream
 * dependencies are deployed first. Static partitions are defined by the dag are enforced. Stram an later on do
 * dynamic optimization.<br>
 * In current implementation optimization is not done with number of containers. The number provided in the dag
 * specification is treated as minimum as well as maximum. Once the optimization layer is built this would change<br>
 * DAG deployment thus blocks successful running of a streaming job in the current version of the streaming platform<br>
 * <br>
 */
public class PhysicalPlan {

  private final static Logger LOG = LoggerFactory.getLogger(PhysicalPlan.class);

  /**
   * Common abstraction for streams and operators for heartbeat/monitoring.<p>
   * <br>
   *
   */
  public abstract static class PTComponent {
    String id;
    PTContainer container;

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
  public static class PTInput {
    final DAG.StreamDecl logicalStream;
    final PTComponent target;
    final byte[] partition;
    final PTComponent source;
    final String portName;

    /**
     *
     * @param logicalStream
     * @param target
     * @param partition
     * @param source
     */
    protected PTInput(String portName, StreamDecl logicalStream, PTComponent target, byte[] partition, PTComponent source) {
      this.logicalStream = logicalStream;
      this.target = target;
      this.partition = partition;
      this.source = source;
      this.portName = portName;
    }

    /**
     *
     * @return String
     */
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("target", this.target).
          append("port", this.portName).
          append("stream", this.logicalStream.getId()).
          toString();
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
  public static class PTOutput {
    final DAG.StreamDecl logicalStream;
    final PTComponent source;
    final String portName;

    /**
     * Constructor
     * @param logicalStream
     * @param source
     */
    protected PTOutput(String portName, StreamDecl logicalStream, PTComponent source) {
      this.logicalStream = logicalStream;
      this.source = source;
      this.portName = portName;
    }

    /**
     *
     * @return String
     */
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("source", this.source).
          append("port", this.portName).
          append("stream", this.logicalStream.getId()).
          toString();
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
  public static class PTOperator extends PTComponent {
    DAG.OperatorWrapper logicalNode;
    List<PTInput> inputs;
    List<PTOutput> outputs;
    LinkedList<Long> checkpointWindows = new LinkedList<Long>();
    int failureCount = 0;

    /**
     *
     * @return Operator
     */
    public OperatorWrapper getLogicalNode() {
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
    List<PTOperator> operators = new ArrayList<PTOperator>();
    String containerId; // assigned to yarn container id
    String host;
    InetSocketAddress bufferServerAddress;
    int restartAttempts;

    /**
     *
     * @return String
     */
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("operators", this.operators).
          toString();
    }
  }

  private final Map<OperatorWrapper, List<PTOperator>> deployedOperators = new LinkedHashMap<OperatorWrapper, List<PTOperator>>();
  private final List<PTContainer> containers = new ArrayList<PTContainer>();
  private final DAG dag;
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
   * @param dag
   */
  public PhysicalPlan(DAG dag) {

    this.dag = dag;
    this.maxContainers = Math.max(dag.getMaxContainerCount(),1);
    LOG.debug("Initializing for {} containers.", this.maxContainers);

    Map<OperatorWrapper, Set<PTOperator>> inlineGroups = new HashMap<OperatorWrapper, Set<PTOperator>>();

    Stack<OperatorWrapper> pendingNodes = new Stack<OperatorWrapper>();
    for (OperatorWrapper n : dag.getAllOperators()) {
      pendingNodes.push(n);
    }

    while (!pendingNodes.isEmpty()) {
      OperatorWrapper n = pendingNodes.pop();

      if (inlineGroups.containsKey(n)) {
        // node already processed as upstream dependency
        continue;
      }

      // look at all input streams to determine partitioning / number of operators
      byte[][] partitions = null;
      boolean upstreamDeployed = true;
      boolean isSingleNodeInstance = true;
      for (StreamDecl s : n.getInputStreams().values()) {
        if (s.getSource() != null && !inlineGroups.containsKey(s.getSource().getOperatorWrapper())) {
          pendingNodes.push(n);
          pendingNodes.push(s.getSource().getOperatorWrapper());
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
          isSingleNodeInstance = false;
        }
      }

      if (upstreamDeployed) {
        // ready to look at this node
        Set<PTOperator> inlineSet = new HashSet<PTOperator>();
        if (isSingleNodeInstance) {
          for (StreamDecl s : n.getInputStreams().values()) {
            if (s.isInline()) {
              // if stream is marked inline, join the upstream operators
              Set<PTOperator> inlineNodes = inlineGroups.get(s.getSource().getOperatorWrapper());
              // empty set for partitioned upstream node
              if (!inlineNodes.isEmpty()) {
                // update group index for each of the member operators
                for (PTOperator upstreamNode : inlineNodes) {
                  inlineSet.add(upstreamNode);
                  inlineGroups.put(upstreamNode.logicalNode, inlineSet);
                }
              }
            }
          }
        }

        // add new physical node(s)
        List<PTOperator> pnodes = new ArrayList<PTOperator>();
        if (partitions != null) {
          // create node per partition
          for (int i = 0; i < partitions.length; i++) {
            PTOperator pNode = createPTOperator(n, partitions[i], pnodes.size());
            pnodes.add(pNode);
          }
        } else {
          // single instance, no partitions
          PTOperator pNode = createPTOperator(n, null, pnodes.size());
          pnodes.add(pNode);
          inlineSet.add(pNode);
        }

        inlineGroups.put(n, inlineSet);
        this.deployedOperators.put(n, pnodes);
      }
    }

    // assign operators to containers
    int groupCount = 0;
    for (Map.Entry<OperatorWrapper, List<PTOperator>> e : deployedOperators.entrySet()) {
      for (PTOperator node : e.getValue()) {
        if (node.container == null) {
          PTContainer container = getContainer((groupCount++) % maxContainers);
          Set<PTOperator> inlineNodes = inlineGroups.get(node.logicalNode);
          if (!inlineNodes.isEmpty()) {
            for (PTOperator inlineNode : inlineNodes) {
              inlineNode.container = container;
              container.operators.add(inlineNode);
              inlineGroups.remove(inlineNode.logicalNode);
            }
          } else {
            node.container = container;
            container.operators.add(node);
          }
        }
      }
    }

  }

  private final AtomicInteger nodeSequence = new AtomicInteger();

  private PTOperator createPTOperator(OperatorWrapper nodeDecl, byte[] partition, int instanceCount) {

    PTOperator pOperator = new PTOperator();
    pOperator.logicalNode = nodeDecl;
    pOperator.inputs = new ArrayList<PTInput>();
    pOperator.outputs = new ArrayList<PTOutput>();
    pOperator.id = ""+nodeSequence.incrementAndGet();

    for (Map.Entry<DAG.InputPortMeta, StreamDecl> inputEntry : nodeDecl.getInputStreams().entrySet()) {
      // find upstream node(s),
      // (can be multiple with partitioning or load balancing)
      StreamDecl streamDecl = inputEntry.getValue();
      if (streamDecl.getSource() != null) {
        List<PTOperator> upstreamNodes = deployedOperators.get(streamDecl.getSource().getOperatorWrapper());
        for (PTOperator upNode : upstreamNodes) {
          // link to upstream output(s) for this stream
          for (PTOutput upstreamOut : upNode.outputs) {
            if (upstreamOut.logicalStream == streamDecl) {
              PTInput input = new PTInput(inputEntry.getKey().getPortName(), streamDecl, pOperator, partition, upNode);
              pOperator.inputs.add(input);
            }
          }
        }
      }
    }

    for (Map.Entry<DAG.OutputPortMeta, StreamDecl> outputEntry : nodeDecl.getOutputStreams().entrySet()) {
      pOperator.outputs.add(new PTOutput(outputEntry.getKey().getPortName(), outputEntry.getValue(), pOperator));
    }

    return pOperator;
  }

  private byte[][] getStreamPartitions(StreamDecl streamConf)
  {
    if (streamConf.getSerDeClass() != null) {
      try {
        SerDe serde = StramUtils.newInstance(streamConf.getSerDeClass());
        byte[][] partitions = serde.getPartitions();
        if (partitions != null) {
          return partitions;
        }
      }
      catch (Exception e) {
        throw new RuntimeException("Failed to get partition info from " + streamConf.getSerDeClass(), e);
      }
    }
    return null;
  }


  protected List<PTContainer> getContainers() {
    return this.containers;
  }

  protected List<PTOperator> getOperators(OperatorWrapper logicalOperator) {
    return this.deployedOperators.get(logicalOperator);
  }

  protected List<OperatorWrapper> getRootOperators() {
    return dag.getRootOperators();
  }

  /**
   * Determine whether downstream operators are deployed inline.
   * (all instances of the logical downstream node are in the same container)
   * @param output
   */
  protected boolean isDownStreamInline(PTOutput output) {
    StreamDecl logicalStream = output.logicalStream;
    for (DAG.InputPortMeta downStreamPort : logicalStream.getSinks()) {
      for (PTOperator downStreamNode : getOperators(downStreamPort.getOperatorWrapper())) {
        if (output.source.container != downStreamNode.container) {
            return false;
        }
      }
    }
    return true;
  }

}

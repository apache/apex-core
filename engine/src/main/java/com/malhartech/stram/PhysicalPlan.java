/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import com.malhartech.api.DAG.StreamDecl;
import com.malhartech.api.Operator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.api.PartitionableOperator;
import com.malhartech.api.PartitionableOperator.Partition;
import com.malhartech.engine.DefaultUnifier;
import com.malhartech.engine.Operators;
import com.malhartech.engine.Operators.PortMappingDescriptor;
import com.malhartech.stram.OperatorPartitions.PartitionImpl;

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
    final List<byte[]> partitions;
    final PTComponent source;
    final String portName;

    /**
     *
     * @param logicalStream
     * @param target
     * @param partition
     * @param source
     */
    protected PTInput(String portName, StreamDecl logicalStream, PTComponent target, List<byte[]> partitions, PTComponent source) {
      this.logicalStream = logicalStream;
      this.target = target;
      this.partitions = partitions;
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
  public class PTOutput {
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
     * Determine whether downstream operators are deployed inline.
     * (all instances of the logical downstream node are in the same container)
     * @param output
     */
    protected boolean isDownStreamInline() {
      StreamDecl logicalStream = this.logicalStream;
      for (DAG.InputPortMeta downStreamPort : logicalStream.getSinks()) {
        for (PTOperator downStreamNode : getOperators(downStreamPort.getOperatorWrapper())) {
          if (this.source.container != downStreamNode.container) {
              return false;
          }
        }
      }
      return true;
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
    enum State {
      NEW,
      PENDING_DEPLOY,
      RUNNING,
      PENDING_UNDEPLOY,
      REMOVED
    }

    State state = State.NEW;

    DAG.OperatorWrapper logicalNode;
    Partition partition;
    Operator merge;
    List<PTInput> inputs;
    List<PTOutput> outputs;
    LinkedList<Long> checkpointWindows = new LinkedList<Long>();
    long recoveryCheckpoint = 0;
    int failureCount = 0;

    /**
     *
     * @return Operator
     */
    public OperatorWrapper getLogicalNode() {
      return this.logicalNode;
    }

    /**
     * Return the most recent checkpoint for this operator,
     * representing the last backup reported.
     * @return long
     */
    public long getRecentCheckpoint() {
      if (checkpointWindows != null && !checkpointWindows.isEmpty())
        return checkpointWindows.getLast();
      return 0;
    }

    /**
     * Return the checkpoint that can be used for recovery. This may not be the
     * most recent checkpoint, depending on downstream state.
     *
     * @return long
     */
   public long getRecoveryCheckpoint() {
     return recoveryCheckpoint;
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
   * Representation of a container for physical objects of DAG to be placed in
   * <p>
   * <br>
   * References the actual container assigned by the resource manager which
   * hosts the streaming operators in the execution layer.<br>
   * The container reference may change throughout the lifecycle of the
   * application due to failure/recovery or scheduler decisions in general. <br>
   *
   */

  public static class PTContainer {
    List<PTOperator> operators = new ArrayList<PTOperator>();
    String containerId; // assigned yarn container id
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

  private final AtomicInteger nodeSequence = new AtomicInteger();
  private final LinkedHashMap<OperatorWrapper, PMapping> logicalToPTOperator = new LinkedHashMap<OperatorWrapper, PMapping>();
  private final List<PTContainer> containers = new ArrayList<PTContainer>();
  private final DAG dag;
  private final PlanContext ctx;
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


  interface PlanContext {
    /**
     * Read committed frozen state of the partition.
     * Dynamic partitioning requires access to committed state so that operators can be split or merged.
     * @param operatorInstance
     * @return
     * @throws IOException
     */
    public PartitionableOperator readCommitted(PTOperator operatorInstance) throws IOException;

    /**
     * Request deployment changes as sequence of undeploy, container start and deploy groups with dependency.
     * @param container
     */
    public void redeploy(Collection<PTOperator> undeploy, Set<PTContainer> startContainers, Collection<PTOperator> deploy);

    public Set<PTOperator> getDependents(Collection<PTOperator> p);

  }

  private static class PMapping {
    private PMapping(OperatorWrapper ow) {
      this.logicalOperator = ow;
    }

    final private OperatorWrapper logicalOperator;
    final private List<PTOperator> partitions = new LinkedList<PTOperator>();
    final private Map<DAG.OutputPortMeta, PTOperator> mergeOperators = new HashMap<DAG.OutputPortMeta, PTOperator>();

    void addPartition(PTOperator p) {
      partitions.add(p);
    }

    private Collection<PTOperator> getAllNodes() {
      if (partitions.size() == 1) {
        return Collections.singletonList(partitions.get(0));
      }
      Collection<PTOperator> c = new ArrayList<PTOperator>(partitions.size() + 1);
      c.addAll(partitions);
      for (PTOperator out : mergeOperators.values()) {
        c.add(out);
      }
      return c;
    }
  }

  /**
   *
   * @param dag
   */
  public PhysicalPlan(DAG dag, PlanContext ctx) {

    this.dag = dag;
    this.ctx = ctx;
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

      boolean upstreamDeployed = true;

      for (StreamDecl s : n.getInputStreams().values()) {
        if (s.getSource() != null && !inlineGroups.containsKey(s.getSource().getOperatorWrapper())) {
          pendingNodes.push(n);
          pendingNodes.push(s.getSource().getOperatorWrapper());
          upstreamDeployed = false;
          break;
        }
      }

      if (upstreamDeployed) {
        // ready to look at this node

        // determine partitioning / number of operators
        PMapping pnodes = new PMapping(n);
        if (n.getOperator() instanceof PartitionableOperator) {
          initPartitioning(pnodes);
        }

        Set<PTOperator> inlineSet = new HashSet<PTOperator>();
        if (pnodes.partitions.isEmpty()) {
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
          // single instance, no partitions
          PTOperator pNode = addPTOperator(pnodes, null);
          inlineSet.add(pNode);
        }

        inlineGroups.put(n, inlineSet);
        this.logicalToPTOperator.put(n, pnodes);
      }
    }

    // assign operators to containers
    int groupCount = 0;
    for (Map.Entry<OperatorWrapper, PMapping> e : logicalToPTOperator.entrySet()) {
      for (PTOperator node : e.getValue().getAllNodes()) {
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

  private void initPartitioning(PMapping m) {
    PartitionableOperator partitionableOperator = (PartitionableOperator)m.logicalOperator.getOperator();
    List<Partition> partitions = new ArrayList<Partition>(1);
    partitions.add(new PartitionImpl(partitionableOperator));

    // operator to provide initial partitioning
    partitions = partitionableOperator.definePartitions(partitions);
    if (partitions == null || partitions.isEmpty()) {
      throw new IllegalArgumentException("PartitionableOperator must return at least one partition: " + m.logicalOperator);
    }

    // initialize partition merge
    for (Map.Entry<DAG.OutputPortMeta, StreamDecl> outputEntry : m.logicalOperator.getOutputStreams().entrySet()) {
      // get output ports - we expect exactly one port
      Unifier<?> unifier = outputEntry.getKey().getUnifier();
      if (unifier == null) {
        LOG.debug("Using default unifier for {}", outputEntry.getKey());
        unifier = new DefaultUnifier();
      }
      PortMappingDescriptor mergeDesc = new PortMappingDescriptor();
      Operators.describe(unifier, mergeDesc);
      if (mergeDesc.outputPorts.size() != 1) {
        throw new IllegalArgumentException("Merge operator should have single output port, found: " + mergeDesc.outputPorts);
      }
      PTOperator merge = null;
      merge = new PTOperator();
      merge.logicalNode = m.logicalOperator;
      merge.inputs = new ArrayList<PTInput>();
      merge.outputs = new ArrayList<PTOutput>();
      merge.id = ""+nodeSequence.incrementAndGet();
      merge.merge = unifier;
      merge.outputs.add(new PTOutput(mergeDesc.outputPorts.keySet().iterator().next(), outputEntry.getValue(), merge));
      m.mergeOperators.put(outputEntry.getKey(), merge);
    }

    // create operator instance per partition
    for (Partition p : partitions) {
      addPTOperator(m, p);
    }

  }

  private void redoPartitions(OperatorWrapper n) {
    // collect current partitions with committed operator state
    // those will be needed by the partitioner for split/merge
    List<PTOperator> operators = getOperators(n);
    List<PartitionImpl> currentPartitions = new ArrayList<PartitionImpl>(operators.size());
    Map<PartitionableOperator, PTOperator> currentPartitionMap = new HashMap<PartitionableOperator, PTOperator>(operators.size());

    for (PTOperator pOperator : operators) {
      Partition p = pOperator.partition;
      if (p == null) {
        throw new AssertionError("Null partition: " + pOperator);
      }
      // load operator state from last committed checkpoint
      PartitionableOperator partitionedOperator = p.getOperator();
      if (pOperator.recoveryCheckpoint != 0) {
        try {
          partitionedOperator = ctx.readCommitted(pOperator);
        } catch (IOException e) {
          LOG.warn("Failed to read partition state for " + pOperator, e);
          return; // TODO
        }
      }
      // assume it does not matter which instance's port objects are referenced in mapping
      PartitionImpl partition = new PartitionImpl(partitionedOperator, p.getPartitionKeys());
      currentPartitions.add(partition);
      currentPartitionMap.put(partitionedOperator, pOperator);
    }

    // TODO: figure out how to deal with load indicator
    List<Partition> newPartitions = ((PartitionableOperator)n.getOperator()).definePartitions(currentPartitions);

    List<Partition> addedPartitions = new ArrayList<Partition>();
    Set<PTOperator> undeployOperators = this.ctx.getDependents(currentPartitionMap.values());
    // determine modifications of partition set, identify affected operator instance
    for (Partition newPartition : newPartitions) {
      PTOperator op = currentPartitionMap.remove(newPartition.getOperator());
      if (op == null) {
        addedPartitions.add(newPartition);
      } else {
        // check whether mapping was changed
        for (PartitionImpl pi : currentPartitions) {
          if (pi == newPartition && pi.isModified()) {
            // partition was changed
            addedPartitions.add(newPartition);
            undeployOperators.add(op);
          }
        }
      }
    }

    // remaining entries represent deprecated partitions
    undeployOperators.addAll(currentPartitionMap.values());

    // operators need to be removed from deployment and plan first
    // freed resources available prior to new/modified partition processing
    PMapping newMapping = new PMapping(n);
    newMapping.partitions.addAll(this.logicalToPTOperator.get(n).partitions);
    newMapping.mergeOperators.putAll(this.logicalToPTOperator.get(n).mergeOperators);

    for (PTOperator p : undeployOperators) {
      removePTOperator(p);
      newMapping.partitions.remove(p);
      // TODO: remove checkpoint state
    }

    // add new operators after cleanup complete
    List<PTOperator> addedOperators = new ArrayList<PTOperator>(addedPartitions.size());
    Set<PTContainer> newContainers = new HashSet<PTContainer>();

    for (Partition newPartition : addedPartitions) {
      // new partition, add operator instance
      PTOperator p = addPTOperator(newMapping, newPartition);
      addedOperators.add(p);

      // TODO: write checkpoint state
      // set checkpoint on new operators to pin downstream state

      // find container for new operator
      PTContainer c = findContainer(p);
      if (c == null) {
        // get new container
        c = new PTContainer();
        newContainers.add(c);
      }
      p.container = c;
      p.container.operators.add(p); // TODO: thread safety
    }

    Set<PTOperator> deployOperators = this.ctx.getDependents(addedOperators);
    ctx.redeploy(undeployOperators, newContainers, deployOperators);

    this.logicalToPTOperator.put(n, newMapping);  // TODO: thread safety

  }

  private PTContainer findContainer(PTOperator p) {
    // TODO: find container based on current utilization
    return null;
  }

  private PTOperator addPTOperator(PMapping nodeDecl, Partition partition) {

    PTOperator pOperator = new PTOperator();
    pOperator.logicalNode = nodeDecl.logicalOperator;
    pOperator.inputs = new ArrayList<PTInput>();
    pOperator.outputs = new ArrayList<PTOutput>();
    pOperator.id = ""+nodeSequence.incrementAndGet();
    pOperator.partition = partition;

    Map<DAG.InputPortMeta, List<byte[]>> partitionKeys = Collections.emptyMap();
    if (partition != null) {
      partitionKeys = new HashMap<DAG.InputPortMeta, List<byte[]>>(partition.getPartitionKeys().size());
      Map<InputPort<?>, List<byte[]>> partKeys = partition.getPartitionKeys();
      for (Map.Entry<InputPort<?>, List<byte[]>> portEntry : partKeys.entrySet()) {
        DAG.InputPortMeta pportMeta = nodeDecl.logicalOperator.getInputPortMeta(portEntry.getKey());
        if (pportMeta == null) {
          throw new IllegalArgumentException("Invalid port reference " + portEntry);
        }
        partitionKeys.put(pportMeta, portEntry.getValue());
      }
    }

    for (Map.Entry<DAG.InputPortMeta, StreamDecl> inputEntry : nodeDecl.logicalOperator.getInputStreams().entrySet()) {
      // find upstream node(s), (can be multiple partitions)
      StreamDecl streamDecl = inputEntry.getValue();
      if (streamDecl.getSource() != null) {
        PMapping upstream = logicalToPTOperator.get(streamDecl.getSource().getOperatorWrapper());
        Collection<PTOperator> upstreamNodes = upstream.partitions;
        if (!upstream.mergeOperators.isEmpty()) {
          // partitioned input with merge operator
          upstreamNodes = upstream.mergeOperators.values();
        }
        for (PTOperator upNode : upstreamNodes) {
          // link to upstream output(s) for this stream
          for (PTOutput upstreamOut : upNode.outputs) {
            if (upstreamOut.logicalStream == streamDecl) {
              // TODO: look for unifier in upstream output port
              PTInput input = new PTInput(inputEntry.getKey().getPortName(), streamDecl, pOperator, partitionKeys.get(inputEntry.getKey()), upNode);
              pOperator.inputs.add(input);
            }
          }
        }
      }
    }

    for (Map.Entry<DAG.OutputPortMeta, StreamDecl> outputEntry : nodeDecl.logicalOperator.getOutputStreams().entrySet()) {
      pOperator.outputs.add(new PTOutput(outputEntry.getKey().getPortName(), outputEntry.getValue(), pOperator));
      // if a unifier is defined, add the new partition to inputs
      PTOperator mergeNode = nodeDecl.mergeOperators.get(outputEntry.getKey());
      if (mergeNode != null) {
        // add merge operator input
        PTInput input = new PTInput("<merge#" + outputEntry.getKey().getPortName() + ">", outputEntry.getValue(), pOperator, null, pOperator);
        mergeNode.inputs.add(input);
      } else {
        if (!nodeDecl.partitions.isEmpty()) {
          throw new AssertionError("Multiple partitions w/o merge operator: " + nodeDecl.logicalOperator);
        }
      }
    }

    nodeDecl.addPartition(pOperator);
    return pOperator;
  }

  private void removePTOperator(PTOperator node) {
    OperatorWrapper nodeDecl = node.logicalNode;
    PMapping mapping = logicalToPTOperator.get(node.logicalNode);
    for (Map.Entry<DAG.OutputPortMeta, StreamDecl> outputEntry : nodeDecl.getOutputStreams().entrySet()) {
      PTOperator merge = mapping.mergeOperators.get(outputEntry.getKey());
      if (merge != null) {
        List<PTInput> newInputs = new ArrayList<PTInput>(merge.inputs.size());
        for (PTInput sinkIn : merge.inputs) {
          if (sinkIn.source != node) {
            newInputs.add(sinkIn);
          }
        }
        merge.inputs = newInputs;
      } else {
        StreamDecl streamDecl = outputEntry.getValue();
        for (DAG.InputPortMeta inp : streamDecl.getSinks()) {
          List<PTOperator> sinkNodes = logicalToPTOperator.get(inp.getOperatorWrapper()).partitions;
          for (PTOperator sinkNode : sinkNodes) {
            // unlink from downstream operators
            List<PTInput> newInputs = new ArrayList<PTInput>(sinkNode.inputs.size());
            for (PTInput sinkIn : sinkNode.inputs) {
              if (sinkIn.source != node) {
                newInputs.add(sinkIn);
              }
            }
            sinkNode.inputs = newInputs;
          }
        }
      }
    }
  }

  protected List<PTContainer> getContainers() {
    return this.containers;
  }

  protected List<PTOperator> getOperators(OperatorWrapper logicalOperator) {
    return this.logicalToPTOperator.get(logicalOperator).partitions;
  }

  protected List<OperatorWrapper> getRootOperators() {
    return dag.getRootOperators();
  }

}

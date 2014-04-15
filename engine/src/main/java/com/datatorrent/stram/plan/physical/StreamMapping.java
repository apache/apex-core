/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.physical;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.engine.DefaultUnifier;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.Operators;
import com.datatorrent.stram.plan.logical.Operators.PortMappingDescriptor;
import com.datatorrent.stram.plan.physical.PTOperator.PTInput;
import com.datatorrent.stram.plan.physical.PTOperator.PTOutput;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Encapsulates the mapping of input to output operators, including unifiers. Depending on logical plan setting and
 * number of partitions, unifiers are created as needed and potentially cascaded.
 *
 * @since 0.9.0
 */
public class StreamMapping implements java.io.Serializable
{
  private static final long serialVersionUID = 8572852828117485193L;

  private final static Logger LOG = LoggerFactory.getLogger(StreamMapping.class);

  private final StreamMeta streamMeta;
  private final PhysicalPlan plan;
  PTOperator finalUnifier;
  final Set<PTOperator> cascadingUnifiers = Sets.newHashSet();
  private final List<PTOutput> upstream = Lists.newArrayList();


  public StreamMapping(StreamMeta streamMeta, PhysicalPlan plan) {
    this.streamMeta = streamMeta;
    this.plan = plan;
  }

  void addTo(Collection<PTOperator> opers) {
    if (finalUnifier != null) {
      opers.add(finalUnifier);
    }
    opers.addAll(cascadingUnifiers);
  }

  public void setSources(Collection<PTOperator> partitions) {
    upstream.clear();
    // add existing inputs
    for (PTOperator uoper : partitions) {
      for (PTOutput source : uoper.outputs) {
        if (source.logicalStream == streamMeta) {
          upstream.add(source);
        }
      }
    }
    redoMapping();
  }

  private PTOperator createUnifier()
  {
    Unifier<?> unifier = streamMeta.getSource().getUnifier();
    LOG.debug("User supplied unifier is {}", unifier);
    if (unifier == null) {
      LOG.debug("Using default unifier for {}", streamMeta.getSource());
      unifier = new DefaultUnifier();
    }
    PortMappingDescriptor mergeDesc = new PortMappingDescriptor();
    Operators.describe(unifier, mergeDesc);
    if (mergeDesc.outputPorts.size() != 1) {
      throw new AssertionError("Unifier should have single output port, found: " + mergeDesc.outputPorts);
    }

    OperatorMeta om = streamMeta.getSource().getOperatorWrapper();
    PTOperator pu = plan.newOperator(om, om.getName() + "#merge#" + streamMeta.getSource().getPortName());

    pu.unifierClass = unifier.getClass();
    pu.outputs.add(new PTOutput(mergeDesc.outputPorts.keySet().iterator().next(), streamMeta, pu));
    plan.newOpers.put(pu, unifier);
    return pu;
  }

  private List<PTOutput> setupCascadingUnifiers(List<PTOutput> upstream, List<PTOperator> pooledUnifiers, int limit, int level) {
    List<PTOutput> nextLevel = Lists.newArrayList();
    PTOperator pu = null;
    for (int i=0; i<upstream.size(); i++) {
      if (i % limit == 0) {
        if (!pooledUnifiers.isEmpty()) {
          pu = pooledUnifiers.remove(0);
        } else {
          pu = createUnifier();
        }
        assert (pu.outputs.size() == 1) : "unifier has single output";
        nextLevel.addAll(pu.outputs);
        this.cascadingUnifiers.add(pu);
      }

      PTOutput source = upstream.get(i);
      addInput(pu, source, null);
    }

    if (nextLevel.size() > limit) {
      return setupCascadingUnifiers(nextLevel, pooledUnifiers, limit, level);
    } else {
      return nextLevel;
    }
  }

  /**
   * rebuild the tree, which may cause more changes to execution layer than need be
   * TODO: investigate incremental logic
   */
  public void redoMapping() {

    Set<Pair<PTOperator, InputPortMeta>> downstreamOpers = Sets.newHashSet();

    // figure out the downstream consumers
    for (InputPortMeta ipm : streamMeta.getSinks()) {
      // gets called prior to all logical operators mapped
      // skipped for parallel partitions - those are handled elsewhere
      if (!ipm.getValue(PortContext.PARTITION_PARALLEL) && plan.hasMapping(ipm.getOperatorWrapper())) {
        List<PTOperator> partitions = plan.getOperators(ipm.getOperatorWrapper());
        for (PTOperator doper : partitions) {
          downstreamOpers.add(new Pair<PTOperator, InputPortMeta>(doper, ipm));
        }
      }
    }

    if (!downstreamOpers.isEmpty()) {
      // unifiers are required
      for (PTOperator unifier : this.cascadingUnifiers) {
        detachUnifier(unifier);
      }
      if (this.finalUnifier != null) {
        detachUnifier(finalUnifier);
      }

      List<PTOperator> currentUnifiers = Lists.newArrayList(this.cascadingUnifiers);
      this.cascadingUnifiers.clear();
      plan.undeployOpers.addAll(currentUnifiers);

      int limit = streamMeta.getSource().getValue(PortContext.UNIFIER_LIMIT);

      List<PTOutput> unifierSources = this.upstream;
      if (limit > 1 && this.upstream.size() > limit) {
        // cascading unifier
        unifierSources = setupCascadingUnifiers(this.upstream, currentUnifiers, limit, 0);
      }

      // remove remaining unifiers
      for (PTOperator oper : currentUnifiers) {
        plan.removePTOperator(oper);
      }


      // link the downstream operators with the unifiers
      for (Pair<PTOperator, InputPortMeta> doperEntry : downstreamOpers) {

        Map<LogicalPlan.InputPortMeta, PartitionKeys> partKeys = doperEntry.first.partitionKeys;
        PartitionKeys pks = partKeys != null ? partKeys.get(doperEntry.second) : null;

        if (upstream.size() > 1) {
          if (pks == null || pks.mask == 0) {
            if (finalUnifier == null) {
              finalUnifier = createUnifier();
            }
            setInput(doperEntry.first, doperEntry.second, finalUnifier, null);
            for (PTOutput out : unifierSources) {
              addInput(this.finalUnifier, out, null);
            }
          } else {
            // MxN partitioning: unifier per downstream partition
            LOG.debug("MxN unifier for {} {} {}", new Object[] {doperEntry.first, doperEntry.second.getPortName(), pks});
            PTOperator unifier = doperEntry.first.upstreamMerge.get(doperEntry.second);
            if (unifier == null) {
              unifier = createUnifier();
              doperEntry.first.upstreamMerge.put(doperEntry.second, unifier);
              setInput(doperEntry.first, doperEntry.second, unifier, null);
            }
            // sources may change dynamically, rebuild inputs (as for cascading unifiers)
            for (PTInput in : unifier.inputs) {
              in.source.sinks.remove(in);
            }
            unifier.inputs.clear();
            // add new inputs
            for (PTOutput out : unifierSources) {
              addInput(unifier, out, pks);
            }
          }
        } else {
          // no partitioning
          PTOperator unifier = doperEntry.first.upstreamMerge.remove(doperEntry.second);
          if (unifier != null) {
            plan.removePTOperator(unifier);
          }
          setInput(doperEntry.first, doperEntry.second, upstream.get(0).source, pks);
        }
      }

      // Remove the unattached final unifier
      // Unattached final unifier is from
      // 1) Upstream operator partitions are scaled down to one. (no unifier needed)
      // 2) Downstream operators partitions are scaled up from one to multiple. (replaced by merged unifier)
      if (finalUnifier != null && finalUnifier.inputs.isEmpty()) {
          plan.removePTOperator(finalUnifier);
          finalUnifier = null;
      }

    }

  }

  private void setInput(PTOperator oper, InputPortMeta ipm, PTOperator sourceOper, PartitionKeys pks) {
    // TODO: see if this can be handled more efficiently
    for (PTInput in : oper.inputs) {
      if (in.source.source == sourceOper && in.logicalStream == streamMeta && ipm.getPortName().equals(in.portName)) {
        return;
      }
    }
    // link to upstream output(s) for this stream
    for (PTOutput upstreamOut : sourceOper.outputs) {
      if (upstreamOut.logicalStream == streamMeta) {
        PTInput input = new PTInput(ipm.getPortName(), streamMeta, oper, pks, upstreamOut);
        oper.inputs.add(input);
      }
    }
  }

  private void addInput(PTOperator target, PTOutput upstreamOut, PartitionKeys pks) {
    StreamMeta streamMeta = upstreamOut.logicalStream;
    PTInput input = new PTInput("<merge#" + streamMeta.getSource().getPortName() + ">", streamMeta, target, pks, upstreamOut);
    target.inputs.add(input);
  }

  private void detachUnifier(PTOperator unifier) {
    // remove existing unifiers from downstream inputs
    for (PTOutput out : unifier.outputs) {
      for (PTInput input : out.sinks) {
        input.target.inputs.remove(input);
      }
      out.sinks.clear();
    }
    // remove from upstream outputs
    for (PTInput in : unifier.inputs) {
      in.source.sinks.remove(in);
    }
    unifier.inputs.clear();
  }

}

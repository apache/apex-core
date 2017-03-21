/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.plan.physical;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.StreamCodec;

import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.logical.Operators;
import com.datatorrent.stram.plan.logical.Operators.PortMappingDescriptor;
import com.datatorrent.stram.plan.physical.PTOperator.PTInput;
import com.datatorrent.stram.plan.physical.PTOperator.PTOutput;

/**
 * Encapsulates the mapping of input to output operators, including unifiers. Depending on logical plan setting and
 * number of partitions, unifiers are created as needed and potentially cascaded.
 *
 * @since 0.9.0
 */
public class StreamMapping implements java.io.Serializable
{
  private static final long serialVersionUID = 8572852828117485193L;

  private static final Logger LOG = LoggerFactory.getLogger(StreamMapping.class);

  private final StreamMeta streamMeta;
  private final PhysicalPlan plan;
  PTOperator finalUnifier;
  final Set<PTOperator> cascadingUnifiers = Sets.newHashSet();
  final Set<PTOperator> slidingUnifiers = Sets.newHashSet();
  private final List<PTOutput> upstream = Lists.newArrayList();

  public StreamMapping(StreamMeta streamMeta, PhysicalPlan plan)
  {
    this.streamMeta = streamMeta;
    this.plan = plan;
  }

  void addTo(Collection<PTOperator> opers)
  {
    if (finalUnifier != null) {
      opers.add(finalUnifier);
    }
    opers.addAll(cascadingUnifiers);
    opers.addAll(slidingUnifiers);
  }

  public void setSources(Collection<PTOperator> partitions)
  {
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

  public static PTOperator createSlidingUnifier(StreamMeta streamMeta, PhysicalPlan plan, int
      operatorApplicationWindowCount, int slidingWindowCount)
  {
    int gcd = IntMath.gcd(operatorApplicationWindowCount, slidingWindowCount);
    OperatorMeta um = streamMeta.getSource()
        .getSlidingUnifier(operatorApplicationWindowCount / gcd, gcd, slidingWindowCount / gcd);
    PTOperator pu = plan.newOperator(um, um.getName());

    Operator unifier = um.getOperator();
    PortMappingDescriptor mergeDesc = new PortMappingDescriptor();
    Operators.describe(unifier, mergeDesc);
    if (mergeDesc.outputPorts.size() != 1) {
      throw new AssertionError("Unifier must have a single output port, instead found : " + mergeDesc.outputPorts);
    }
    pu.unifiedOperatorMeta = streamMeta.getSource().getOperatorMeta();
    pu.outputs.add(new PTOutput(mergeDesc.outputPorts.keySet().iterator().next(), streamMeta, pu));
    plan.newOpers.put(pu, unifier);
    return pu;
  }

  public static PTOperator createUnifier(StreamMeta streamMeta, PhysicalPlan plan)
  {
    OperatorMeta um = streamMeta.getSource().getUnifierMeta();
    PTOperator pu = plan.newOperator(um, um.getName());

    Operator unifier = um.getOperator();
    PortMappingDescriptor mergeDesc = new PortMappingDescriptor();
    Operators.describe(unifier, mergeDesc);
    if (mergeDesc.outputPorts.size() != 1) {
      throw new AssertionError("Unifier must have a single output port, instead found : " + mergeDesc.outputPorts);
    }

    pu.unifiedOperatorMeta = streamMeta.getSource().getOperatorMeta();
    pu.outputs.add(new PTOutput(mergeDesc.outputPorts.keySet().iterator().next(), streamMeta, pu));
    plan.newOpers.put(pu, unifier);
    return pu;
  }

  private void addSlidingUnifiers()
  {
    OperatorMeta sourceOM = streamMeta.getSource().getOperatorMeta();
    if (sourceOM.getAttributes().contains(Context.OperatorContext.SLIDE_BY_WINDOW_COUNT)) {
      if (sourceOM.getValue(Context.OperatorContext.SLIDE_BY_WINDOW_COUNT) <
          sourceOM.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)) {
        plan.undeployOpers.addAll(slidingUnifiers);
        slidingUnifiers.clear();
        List<PTOutput> newUpstream = Lists.newArrayList();
        PTOperator slidingUnifier;
        for (PTOutput source : upstream) {
          slidingUnifier = StreamMapping.createSlidingUnifier(streamMeta, plan,
              sourceOM.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT),
              sourceOM.getValue(Context.OperatorContext.SLIDE_BY_WINDOW_COUNT));
          addInput(slidingUnifier, source, null);
          this.slidingUnifiers.add(slidingUnifier);
          newUpstream.add(slidingUnifier.outputs.get(0));
        }
        upstream.clear();
        upstream.addAll(newUpstream);
      } else {
        LOG.warn("Sliding Window Count {} should be less than APPLICATION WINDOW COUNT {}", sourceOM.getValue(Context.OperatorContext.SLIDE_BY_WINDOW_COUNT), sourceOM.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT));
      }
    }
  }

  @SuppressWarnings("AssignmentToForLoopParameter")
  private List<PTOutput> setupCascadingUnifiers(List<PTOutput> upstream, List<PTOperator> pooledUnifiers, int limit, int level)
  {
    List<PTOutput> nextLevel = Lists.newArrayList();
    PTOperator pu = null;
    for (int i = 0; i < upstream.size(); i++) {
      if (i % limit == 0) {
        if (upstream.size() - i < limit) {
          while (i < upstream.size()) {
            nextLevel.add(upstream.get(i));
            i++;
          }
          continue;
        }
        if (!pooledUnifiers.isEmpty()) {
          pu = pooledUnifiers.remove(0);
        } else {
          pu = createUnifier(streamMeta, plan);
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
  private void redoMapping()
  {

    Set<Pair<PTOperator, InputPortMeta>> downstreamOpers = Sets.newHashSet();

    // figure out the downstream consumers
    for (InputPortMeta ipm : streamMeta.getSinks()) {
      // gets called prior to all logical operators mapped
      // skipped for parallel partitions - those are handled elsewhere
      if (!ipm.getValue(PortContext.PARTITION_PARALLEL) && plan.hasMapping(ipm.getOperatorMeta())) {
        List<PTOperator> partitions = plan.getOperators(ipm.getOperatorMeta());
        for (PTOperator doper : partitions) {
          downstreamOpers.add(new Pair<>(doper, ipm));
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
      addSlidingUnifiers();

      int limit = streamMeta.getSource().getValue(PortContext.UNIFIER_LIMIT);

      boolean separateUnifiers = false;
      Integer lastId = null;
      for (InputPortMeta ipm : streamMeta.getSinks()) {
        Integer id = plan.getStreamCodecIdentifier(ipm.getStreamCodec());
        if (lastId == null) {
          lastId = id;
        } else if (!id.equals(lastId)) {
          separateUnifiers = true;
          break;
        }
      }

      List<PTOutput> unifierSources = this.upstream;
      Map<StreamCodec<?>, List<PTOutput>> cascadeUnifierSourcesMap = Maps.newHashMap();

      if (limit > 1 && this.upstream.size() > limit) {
        // cascading unifier
        if (!separateUnifiers) {
          unifierSources = setupCascadingUnifiers(this.upstream, currentUnifiers, limit, 0);
        } else {
          for (InputPortMeta ipm : streamMeta.getSinks()) {
            StreamCodec<?> streamCodec = ipm.getStreamCodec();
            if (!cascadeUnifierSourcesMap.containsKey(streamCodec)) {
              unifierSources = setupCascadingUnifiers(this.upstream, currentUnifiers, limit, 0);
              cascadeUnifierSourcesMap.put(streamCodec, unifierSources);
            }
          }
        }
      }

      // remove remaining unifiers
      for (PTOperator oper : currentUnifiers) {
        plan.removePTOperator(oper);
      }

      // Directly getting attribute from map to know if it is set or not as it can be overriden by the input
      Boolean sourceSingleFinal = streamMeta.getSource().getAttributes().get(PortContext.UNIFIER_SINGLE_FINAL);

      // link the downstream operators with the unifiers
      for (Pair<PTOperator, InputPortMeta> doperEntry : downstreamOpers) {

        Map<LogicalPlan.InputPortMeta, PartitionKeys> partKeys = doperEntry.first.partitionKeys;
        PartitionKeys pks = partKeys != null ? partKeys.get(doperEntry.second) : null;
        Boolean sinkSingleFinal = doperEntry.second.getAttributes().get(PortContext.UNIFIER_SINGLE_FINAL);
        boolean lastSingle = (sinkSingleFinal != null) ? sinkSingleFinal :
            (sourceSingleFinal != null ? sourceSingleFinal.booleanValue() : PortContext.UNIFIER_SINGLE_FINAL.defaultValue);

        if (upstream.size() > 1) {
          // detach downstream from upstream operator for the case where no unifier existed previously
          for (PTOutput source : upstream) {
            Iterator<PTInput> sinks = source.sinks.iterator();
            while (sinks.hasNext()) {
              PTInput sink = sinks.next();
              if (sink.target == doperEntry.first) {
                doperEntry.first.inputs.remove(sink);
                sinks.remove();
              }
            }
          }
          if (!separateUnifiers && lastSingle) {
            if (finalUnifier == null) {
              finalUnifier = createUnifier(streamMeta, plan);
            }
            setInput(doperEntry.first, doperEntry.second, finalUnifier, (pks == null) || (pks.mask == 0) ? null : pks);
            if (finalUnifier.inputs.isEmpty()) {
              // set unifier inputs once, regardless how many downstream operators there are
              for (PTOutput out : unifierSources) {
                addInput(this.finalUnifier, out, null);
              }
            }
          } else {
            // MxN partitioning: unifier per downstream partition
            LOG.debug("MxN unifier for {} {} {}", new Object[] {doperEntry.first, doperEntry.second.getPortName(), pks});
            PTOperator unifier = doperEntry.first.upstreamMerge.get(doperEntry.second);
            if (unifier == null) {
              unifier = createUnifier(streamMeta, plan);
              doperEntry.first.upstreamMerge.put(doperEntry.second, unifier);
              setInput(doperEntry.first, doperEntry.second, unifier, null);
            }
            // sources may change dynamically, rebuild inputs (as for cascading unifiers)
            for (PTInput in : unifier.inputs) {
              in.source.sinks.remove(in);
            }
            unifier.inputs.clear();
            List<PTOutput> doperUnifierSources = unifierSources;
            if (separateUnifiers) {
              List<PTOutput> cascadeSources = cascadeUnifierSourcesMap.get(doperEntry.second.getStreamCodec());
              if (cascadeSources != null) {
                doperUnifierSources = cascadeSources;
              }
            }
            // add new inputs
            for (PTOutput out : doperUnifierSources) {
              addInput(unifier, out, (pks == null) || (pks.mask == 0) ? null : pks);
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

  private void setInput(PTOperator oper, InputPortMeta ipm, PTOperator sourceOper, PartitionKeys pks)
  {
    // TODO: see if this can be handled more efficiently
    for (PTInput in : oper.inputs) {
      if (in.source.source == sourceOper && in.logicalStream == streamMeta && ipm.getPortName().equals(in.portName)) {
        return;
      }
    }
    // link to upstream output(s) for this stream
    for (PTOutput upstreamOut : sourceOper.outputs) {
      if (upstreamOut.logicalStream == streamMeta) {
        PTInput input = new PTInput(ipm.getPortName(), streamMeta, oper, pks, upstreamOut, ipm.getValue(LogicalPlan.IS_CONNECTED_TO_DELAY_OPERATOR));
        oper.inputs.add(input);
      }
    }
  }

  public static void addInput(PTOperator target, PTOutput upstreamOut, PartitionKeys pks)
  {
    StreamMeta lStreamMeta = upstreamOut.logicalStream;
    PTInput input = new PTInput("<merge#" + lStreamMeta.getSource().getPortName() + ">", lStreamMeta, target, pks, upstreamOut, false);
    target.inputs.add(input);
  }

  private void detachUnifier(PTOperator unifier)
  {
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

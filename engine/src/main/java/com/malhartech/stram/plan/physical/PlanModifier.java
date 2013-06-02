package com.malhartech.stram.plan.physical;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;
import com.malhartech.api.Operator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.stram.PhysicalPlan;
import com.malhartech.stram.PhysicalPlan.PTContainer;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.plan.logical.LogicalPlan;
import com.malhartech.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.malhartech.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.malhartech.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.malhartech.stram.plan.logical.LogicalPlan.StreamMeta;
import com.malhartech.stram.plan.logical.Operators;

public class PlanModifier {

  final private LogicalPlan logicalPlan;
  final private PhysicalPlan physicalPlan;
  final private PhysicalPlan.PlanContext physicalPlanContext;

  private final Set<PTOperator> affectedOperators = Sets.newHashSet();
  private final Set<OperatorMeta> newLogicalOperators = Sets.newHashSet();

  public PlanModifier(PhysicalPlan plan, PhysicalPlan.PlanContext ctx) {
    this.physicalPlan = plan;
    this.logicalPlan = plan.getDAG();
    this.physicalPlanContext = ctx;
  }

  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<?>... sinks) {

    StreamMeta sm = logicalPlan.getStream(id);
    if (sm != null) {
      if (sm.getSource().getOperatorWrapper().getMeta(source) != sm.getSource()) {
        throw new AssertionError(String.format("Stream %s already connected to %s", sm, sm.getSource()));
      }
    } else {
      // fails on duplicate stream name
      @SuppressWarnings("unchecked")
      StreamMeta newStream = logicalPlan.addStream(id, source);
      sm = newStream;
    }

    for (Operator.InputPort<?> sink : sinks) {
      sm.addSink(sink);
      if (physicalPlan != null) {
        for (InputPortMeta ipm : sm.getSinks()) {
          if (ipm.getPortObject() == sink) {
            physicalPlan.connectInput(ipm, affectedOperators);
          }
        }
      }
    }
    return sm;
  }

  /**
   * Add stream to logical plan. If a stream with same name and source already
   * exists, it will be connected to the new downstream operator.
   *
   * @param streamName
   * @param sourceOperName
   * @param sourcePortName
   * @param targetOperName
   * @param targetPortName
   */
  public void addStream(String streamName, String sourceOperName, String sourcePortName, String targetOperName, String targetPortName) {

    OperatorMeta om = logicalPlan.getOperatorMeta(sourceOperName);
    if (om == null) {
      throw new AssertionError("Invalid operator name " + sourceOperName);
    }

    Operators.PortMappingDescriptor portMap = new Operators.PortMappingDescriptor();
    Operators.describe(om.getOperator(), portMap);

    OutputPort<?> sourcePort = portMap.outputPorts.get(sourcePortName);
    if (sourcePort == null) {
      throw new AssertionError(String.format("Invalid port %s (%s)", sourcePortName, om));
    }
    addStream(streamName, sourcePort, getInputPort(targetOperName, targetPortName));

  }

  private InputPort<?> getInputPort(String operName, String portName) {
    OperatorMeta om = logicalPlan.getOperatorMeta(operName);
    if (om == null) {
      throw new AssertionError("Invalid operator name " + operName);
    }

    Operators.PortMappingDescriptor portMap = new Operators.PortMappingDescriptor();
    Operators.describe(om.getOperator(), portMap);

    InputPort<?> port = portMap.inputPorts.get(portName);
    if (port == null) {
      throw new AssertionError(String.format("Invalid port %s (%s)", portName, om));
    }
    return port;
  }

  /**
   * Remove the named stream. Ignored when stream does not exist.
   * @param streamName
   */
  public void removeStream(String streamName) {
    StreamMeta sm = logicalPlan.getStream(streamName);
    if (sm == null) {
      return;
    }

    if (physicalPlan != null) {
      physicalPlan.removeLogicalStream(sm, affectedOperators);
    }
    // remove from logical plan
    sm.remove();
  }

  /**
   * Add operator to logical plan.
   * @param name
   */
  public void addOperator(String name, Operator operator) {
    logicalPlan.addOperator(name, operator);
    // add to physical plan after all changes are done
    newLogicalOperators.add(logicalPlan.getMeta(operator));
  }

  /**
   * Process all changes in the logical plan.
   */
  public void applyChanges() {

    Set<PTOperator> newOperators = Sets.newHashSet();
    if (physicalPlan != null) {
      for (OperatorMeta om : this.newLogicalOperators) {
        physicalPlan.addLogicalOperator(om);
        // keep track of new instances
        newOperators.addAll(physicalPlan.getOperators(om));
      }
    }

    // assign containers
    Set<PTContainer> newContainers = Sets.newHashSet();
    physicalPlan.assignContainers(newOperators, newContainers);

    // existing downstream operators require redeploy
    Set<PTOperator> undeployOperators = physicalPlan.getDependents(newOperators);
    undeployOperators.removeAll(newOperators);

    Set<PTOperator> deployOperators = physicalPlan.getDependents(newOperators);

    Set<PTOperator> redeployOperators = physicalPlan.getDependents(this.affectedOperators);
    undeployOperators.addAll(redeployOperators);
    deployOperators.addAll(redeployOperators);

    physicalPlanContext.deploy(Collections.<PTContainer>emptySet(), undeployOperators, newContainers, deployOperators);

  }

}

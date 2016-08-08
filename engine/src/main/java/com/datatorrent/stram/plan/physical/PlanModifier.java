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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.validation.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.StringCodec;
import com.datatorrent.stram.StringCodecs;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.plan.logical.Operators;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;
import com.datatorrent.stram.plan.logical.mod.DAGChangeSetImpl;

/**
 * Modification of the query plan on running application. Will first apply
 * logical plan changes to a copy of the logical plan and then apply changes to
 * the original and physical plan after validation passes.
 *
 * @since 0.3.2
 */
public class PlanModifier
{
  private static final Logger LOG = LoggerFactory.getLogger(PlanModifier.class);
  private final LogicalPlan logicalPlan;
  private final PhysicalPlan physicalPlan;

  /**
   * For dry run on logical plan only
   * @param logicalPlan
   */
  public PlanModifier(LogicalPlan logicalPlan)
  {
    this.logicalPlan = logicalPlan;
    this.physicalPlan = null;
    init();
  }

  /**
   * For modification of physical plan
   * @param plan
   */
  public PlanModifier(PhysicalPlan plan)
  {
    this.physicalPlan = plan;
    this.logicalPlan = plan.getLogicalPlan();
    init();
  }

  private void init()
  {
    Map<Class<?>, Class<? extends StringCodec<?>>> codecs = logicalPlan.getAttributes().get(DAGContext.STRING_CODECS);
    if (codecs != null) {
      StringCodecs.loadConverters(codecs);
    }
  }

  public StreamMeta addSinks(String id, Operator.InputPort<?>... sinks)
  {
    StreamMeta sm = logicalPlan.getStream(id);
    if (sm == null) {
      throw new AssertionError("Stream " + id + " is not found!");
    }
    for (Operator.InputPort<?> sink : sinks) {
      sm.addSink(sink);
      if (physicalPlan != null) {
        for (InputPortMeta ipm : sm.getSinks()) {
          if (ipm.getPortObject() == sink) {
            physicalPlan.connectInput(ipm);
          }
        }
      }
    }
    return sm;
  }

  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<?>... sinks)
  {
    StreamMeta sm = logicalPlan.getStream(id);
    if (sm != null) {
      if (sm.getSource().getOperatorMeta().getMeta(source) != sm.getSource()) {
        throw new AssertionError(String.format("Stream %s already connected to %s", sm, sm.getSource()));
      }
    } else {
      // fails on duplicate stream name
      @SuppressWarnings("unchecked")
      StreamMeta newStream = logicalPlan.addStream(id, source);
      sm = newStream;
    }
    return addSinks(id, sinks);
  }

  /**
   * Add stream to logical plan. If a stream with same name and source already
   * exists, the new downstream operator will be attached to it.
   *
   * @param streamName
   * @param sourceOperName
   * @param sourcePortName
   * @param targetOperName
   * @param targetPortName
   */
  public void addStream(String streamName, String sourceOperName, String sourcePortName, String targetOperName, String targetPortName)
  {
    OperatorMeta om = logicalPlan.getOperatorMeta(sourceOperName);
    if (om == null) {
      throw new ValidationException("Invalid operator name " + sourceOperName);
    }

    Operators.PortMappingDescriptor portMap = new Operators.PortMappingDescriptor();
    Operators.describe(om.getOperator(), portMap);
    PortContextPair<OutputPort<?>> sourcePort = portMap.outputPorts.get(sourcePortName);
    if (sourcePort == null) {
      throw new AssertionError(String.format("Invalid port %s (%s)", sourcePortName, om));
    }
    addStream(streamName, sourcePort.component, getInputPort(targetOperName, targetPortName));
  }

  /**
   * Add sink to an existing stream to logical plan.
   *
   * @param streamName
   * @param targetOperName
   * @param targetPortName
   */
  public void addSink(String streamName, String targetOperName, String targetPortName)
  {
    addSinks(streamName, getInputPort(targetOperName, targetPortName));
  }

  private OperatorMeta assertGetOperator(String operName)
  {
    OperatorMeta om = logicalPlan.getOperatorMeta(operName);
    if (om == null) {
      throw new AssertionError("Invalid operator name " + operName);
    }
    return om;
  }

  private InputPort<?> getInputPort(String operName, String portName)
  {
    OperatorMeta om = assertGetOperator(operName);
    Operators.PortMappingDescriptor portMap = new Operators.PortMappingDescriptor();
    Operators.describe(om.getOperator(), portMap);
    PortContextPair<InputPort<?>> port = portMap.inputPorts.get(portName);
    if (port == null) {
      throw new AssertionError(String.format("Invalid port %s (%s)", portName, om));
    }
    return port.component;
  }

  /**
   * Remove the named stream. Ignored when stream does not exist.
   * @param streamName
   */
  public void removeStream(String streamName)
  {
    StreamMeta sm = logicalPlan.getStream(streamName);
    if (sm == null) {
      return;
    }

    if (physicalPlan != null) {
      // associated operators will redeploy
      physicalPlan.removeLogicalStream(sm);
    }
    // remove from logical plan
    sm.remove();
  }

  /**
   * Add operator to logical plan.
   * @param name
   */
  public void addOperator(String name, Operator operator)
  {
    logicalPlan.addOperator(name, operator);
    // add to physical plan after all changes are done
    if (physicalPlan != null) {
      OperatorMeta om = logicalPlan.getMeta(operator);
      physicalPlan.addLogicalOperator(om);
    }
  }

  /**
   * Remove named operator and any outgoing streams.
   * @param name
   */
  public void removeOperator(String name)
  {
    OperatorMeta om = logicalPlan.getOperatorMeta(name);
    if (om == null) {
      return;
    }

    if (!om.getInputStreams().isEmpty()) {
      for (Map.Entry<InputPortMeta, StreamMeta> input : om.getInputStreams().entrySet()) {
        if (input.getValue().getSinks().size() == 1) {
          // would result in dangling stream
          String msg = String.format("Operator %s connected to input streams %s", om.getName(), om.getInputStreams());
          throw new ValidationException(msg);
        }
      }
    }
    if (!om.getOutputStreams().isEmpty()) {
      String msg = String.format("Operator %s connected to output streams %s", om.getName(), om.getOutputStreams());
      throw new ValidationException(msg);
    }
    /*
    // remove associated sinks
    Map<InputPortMeta, StreamMeta> inputStreams = om.getInputStreams();
    for (Map.Entry<InputPortMeta, StreamMeta> e : inputStreams.entrySet()) {
      if (e.getKey().getOperatorMeta() == om) {
        if (e.getValue().getSinks().size() == 1) {
          // drop stream
          e.getValue().remove();
        }
      }
    }
    */
    logicalPlan.removeOperator(om.getOperator());

    if (physicalPlan != null) {
      physicalPlan.removeLogicalOperator(om);
    }
  }

  /**
   * Set the property on a new operator. Since this is only intended to modify
   * previously added operators, no change to the physical plan is required.
   *
   * @param operatorName
   * @param propertyName
   * @param propertyValue
   */
  public void setOperatorProperty(String operatorName, String propertyName, String propertyValue)
  {
    OperatorMeta om = assertGetOperator(operatorName);
    if (physicalPlan != null) {
      for (PTOperator oper : physicalPlan.getOperators(om)) {
        if (!physicalPlan.newOpers.containsKey(oper)) {
          throw new ValidationException("Properties can only be set on new operators: " + om + " " + propertyName + " " + propertyValue);
        }
      }
    }
    Map<String, String> props = Collections.singletonMap(propertyName, propertyValue);
    LogicalPlanConfiguration.setOperatorProperties(om.getOperator(), props);
  }

  /**
   * Deploy plan changes to execution layer..
   */
  public void applyChanges(PhysicalPlan.PlanContext physicalPlanContext)
  {
    physicalPlan.deployChanges();
  }

  public void applyDagChangeSet(DAG.DAGChangeSet dagChanges)
  {
    DAGChangeSetImpl dag = (DAGChangeSetImpl)dagChanges;
    List<OperatorMeta> orderedOperators = dag.getOperatorsInOrder();
    for (OperatorMeta om : orderedOperators) {
      LOG.info("Adding operator {}", om.getName());
      logicalPlan.addOperator(om.getName(), om.getOperator());
      OperatorMeta newMeta = logicalPlan.getMeta(om.getOperator());
      newMeta.copyAttributesFrom(om);
    }

    for (StreamMeta streamMeta : dag.getAllStreams()) {
      LOG.info("Adding stream {}", streamMeta.getName());
      StreamMeta sm = logicalPlan.getStream(streamMeta.getName());
      if (sm == null) {
        sm = logicalPlan.addStream(streamMeta.getName());
      }
      sm.setSource(streamMeta.getSource().getPortObject());
      for (InputPortMeta sink : streamMeta.getSinks()) {
        sm.addSink(sink.getPortObject());
      }
      sm.setLocality(streamMeta.getLocality());
    }

    /**
     * Add streams which are exended
     */
    for (DAG.StreamMeta sm : dag.getExtendStreams().values()) {
      LOG.info("Extending streams {}", sm.getName());
      StreamMeta origStreamMeta = null;
      Collection<InputPort> inputPorts = null;

      if (sm instanceof DAGChangeSetImpl.StreamExtendBySource) {
        DAGChangeSetImpl.StreamExtendBySource sm1 = (DAGChangeSetImpl.StreamExtendBySource)sm;
        LOG.debug("process StreamExtendBySource name {} operator {} port {}", sm1.getName(), sm1.getOperatorName(), sm1.getPortName());
        origStreamMeta = getSteramBySource(sm1.getOperatorName(), sm1.getPortName());
        // If stream is not present in original dagChanges, then add it.
        if (origStreamMeta == null) {
          origStreamMeta = logicalPlan.addStream(sm.getName());
          origStreamMeta.setSource(getPortObject(sm1.getOperatorName(), sm1.getPortName()));
        }
        inputPorts = sm1.getSinkPorts();
      }

      if (sm instanceof DAGChangeSetImpl.ExtendStreamMeta) {
        DAGChangeSetImpl.ExtendStreamMeta esm = (DAGChangeSetImpl.ExtendStreamMeta)sm;
        origStreamMeta = logicalPlan.getStream(esm.getName());
        if (sm == null) {
          throw new RuntimeException("The stream to extend does not exists in original DAG");
        }
        inputPorts = esm.getSinkPorts();
      }

      if (origStreamMeta != null && inputPorts != null) {
        LOG.debug("populating stream {} with sinks {}", sm.getName(), inputPorts.size());
        for (InputPort port : inputPorts) {
          origStreamMeta.addSink(port);
        }
      }
    }

    if (physicalPlan != null) {

      // start physical plan change
      for (OperatorMeta om : orderedOperators) {
        OperatorMeta newMeta = logicalPlan.getOperatorMeta(om.getName());
        physicalPlan.addLogicalOperator(newMeta);
      }

      for (StreamMeta streamMeta : logicalPlan.getAllStreams()) {
        for (InputPortMeta ipm : streamMeta.getSinks()) {
          physicalPlan.connectInput(ipm);
        }
      }
    }

    for (String name : dag.getRemovedStreams()) {
      LOG.info("Removing stream {}", name);
      removeStream(name);
    }

    for (String name : dag.getRemovedOperators()) {
      LOG.info("Removing operator {}", name);
      removeOperator(name);
    }

    if (physicalPlan != null) {
      physicalPlan.deployChanges();
    }
  }

  StreamMeta getSteramBySource(String operatorName, String portName)
  {
    for (OperatorMeta om : logicalPlan.getAllOperators()) {
      if (om.getName().equals(operatorName)) {
        for (Map.Entry<LogicalPlan.OutputPortMeta, LogicalPlan.StreamMeta> entry : om.getOutputStreams().entrySet()) {
          if (entry.getKey().getPortName().equals(portName)) {
            return entry.getValue();
          }
        }
      }
    }
    return null;
  }

  OutputPort<?> getPortObject(String operatorName, String portName)
  {
    for (OperatorMeta om : logicalPlan.getAllOperators()) {
      if (om.getName().equals(operatorName)) {
        Operators.PortMappingDescriptor descriptor = new Operators.PortMappingDescriptor();
        Operators.describe(om.getOperator(), descriptor);
        PortContextPair<OutputPort<?>> port = descriptor.outputPorts.get(portName);
        return port.component;
      }
    }
    return null;
  }
}

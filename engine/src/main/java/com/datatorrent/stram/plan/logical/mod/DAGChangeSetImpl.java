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
package com.datatorrent.stram.plan.logical.mod;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.Maps;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.DAGChangeSet;
import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * An implementation for DAGChangeSet. Instance of this object will be provided
 * to StatsListener through context, stat listener can use this object to modify
 * existing DAG and return modified DAG to engine.
 */
public class DAGChangeSetImpl extends LogicalPlan implements DAGChangeSet
{
  private List<String> removedOperators = new ArrayList<>();
  private List<String> removedStreams = new ArrayList<>();

  public void removeOperator(String name)
  {
    removedOperators.add(name);
  }

  public void removeStream(String name)
  {
    removedStreams.add(name);
  }

  public List<LogicalPlan.OperatorMeta> getOperatorsInOrder()
  {
    List<LogicalPlan.OperatorMeta> operators = new ArrayList<>();
    Set<LogicalPlan.OperatorMeta> added = new HashSet<>();

    Stack<LogicalPlan.OperatorMeta> pendingNodes = new Stack<>();

    for (LogicalPlan.OperatorMeta n : getAllOperators()) {
      pendingNodes.push(n);
    }

    while (!pendingNodes.isEmpty()) {
      LogicalPlan.OperatorMeta n = pendingNodes.pop();

      if (added.contains(n)) {
        // already processed as upstream dependency
        continue;
      }

      boolean upstreamDeployed = true;

      for (Map.Entry<LogicalPlan.InputPortMeta, LogicalPlan.StreamMeta> entry : n.getInputStreams().entrySet()) {
        LogicalPlan.StreamMeta s = entry.getValue();
        boolean delay = entry.getKey().getValue(IS_CONNECTED_TO_DELAY_OPERATOR);
        // skip delay sources since it's going to be handled as downstream
        if (!delay && s.getSource() != null && !added.contains(s.getSource().getOperatorMeta())) {
          pendingNodes.push(n);
          pendingNodes.push(s.getSource().getOperatorMeta());
          upstreamDeployed = false;
          break;
        }
      }

      if (upstreamDeployed) {
        added.add(n);
        operators.add(n);
      }
    }
    return operators;
  }

  public List<String> getRemovedOperators()
  {
    return removedOperators;
  }

  public List<String> getRemovedStreams()
  {
    return removedStreams;
  }

  /**
   * Base class for StreamMeta object which modify existing stream or
   * add new stream to the DAG with one end point in the existing stream.
   */
  public static class ExtendStreamMeta implements DAG.StreamMeta
  {
    /** name of the stream to change */
    private String name;
    /** new additional sinks to add in stream */
    private Set<Operator.InputPort> sinkPorts = new HashSet<>();

    public ExtendStreamMeta(String id)
    {
      this.name = id;
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public Locality getLocality()
    {
      throw new UnsupportedOperationException("Can not get locality of the existing stream");
    }

    public DAG.StreamMeta setLocality(Locality locality)
    {
      throw new UnsupportedOperationException("Can not change locality of existing stream");
    }

    @Override
    public DAG.StreamMeta setSource(Operator.OutputPort<?> port)
    {
      throw new UnsupportedOperationException("Can not set source of the existing stream");
    }

    @Override
    public DAG.StreamMeta addSink(Operator.InputPort<?> port)
    {
      sinkPorts.add(port);
      return this;
    }

    @Override
    public DAG.StreamMeta persistUsing(String name, Operator persistOperator, Operator.InputPort<?> persistOperatorInputPort)
    {
      throw new UnsupportedOperationException("persist using operation is not supported");
    }

    @Override
    public DAG.StreamMeta persistUsing(String name, Operator persistOperator)
    {
      throw new UnsupportedOperationException("persist using operation is not supported");
    }

    @Override
    public DAG.StreamMeta persistUsing(String name, Operator persistOperator, Operator.InputPort<?> persistOperatorInputPort, Operator.InputPort<?> sinkToPersist)
    {
      return null;
    }

    public Set<Operator.InputPort> getSinkPorts()
    {
      return sinkPorts;
    }

  }

  /**
   * StreamMeta for modification which add a new stream with one end in
   * already existing DAG, and other end is specified as port.
   */
  public static class StreamExtendBySource extends ExtendStreamMeta
  {
    private String operatorName;
    private String portName;

    public StreamExtendBySource(String id, String operator, String port)
    {
      super(id);
      this.operatorName = operator;
      this.portName = port;
    }

    public String getOperatorName()
    {
      return operatorName;
    }

    public void setOperatorName(String operatorName)
    {
      this.operatorName = operatorName;
    }

    public String getPortName()
    {
      return portName;
    }

    public void setPortName(String portName)
    {
      this.portName = portName;
    }
  }

  private Map<String, DAG.StreamMeta> extendStreams = Maps.newHashMap();

  public Map<String, DAG.StreamMeta> getExtendStreams()
  {
    return extendStreams;
  }

  DAG.StreamMeta getExtendStreams(String id)
  {
    DAG.StreamMeta sm = getStream(id);
    if (sm == null) {
      sm = extendStreams.get(id);
    }
    if (sm == null) {
      sm = new ExtendStreamMeta(id);
      extendStreams.put(id, sm);
    }
    return sm;
  }

  /**
   * Extend stream present in original DAG with new Sinks.
   */
  @Override
  public DAG.StreamMeta extendStream(String id, Operator.InputPort... ports)
  {
    DAG.StreamMeta sm = getExtendStreams(id);
    for (Operator.InputPort port : ports) {
      sm.addSink(port);
    }
    return sm;
  }

  /**
   * Add stream to existing DAG which one end-point belong to operator already existing in
   * original DAG.
   */
  @Override
  public DAG.StreamMeta addStream(String id, String operatorName, String portName, Operator.InputPort... ports)
  {
    DAG.StreamMeta sm = getStream(id);
    if (sm != null) {
      throw new IllegalStateException("Stream already connected " + sm);
    }
    sm = extendStreams.get(id);
    if (sm != null) {
      throw new IllegalStateException("Stream already exists");
    }
    sm = new StreamExtendBySource(id, operatorName, portName);
    extendStreams.put(id, sm);
    for (Operator.InputPort port : ports) {
      sm.addSink(port);
    }
    return sm;
  }
}

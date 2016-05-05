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
package com.datatorrent.api;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.GenericOperator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.Unifier;

/**
 * A Module is a component which can be added to the DAG similar to the operator,
 * using addModule API. The module should implement populateDAG method, which
 * will be called by the platform, and DAG populated by the module will be
 * replace in place of the module.
 *
 *
 * @since 3.3.0
 */
@InterfaceStability.Evolving
public interface Module extends GenericOperator
{
  void populateDAG(DAG dag, Configuration conf);

  /**
   * These ports allow platform to short circuit module port to the operator port. i.e When a module is expanded, it can
   * specify  which operator's port is used to replaced the module port in the final DAG.
   *
   * @param <T> data type accepted at the input port.
   */
  final class ProxyInputPort<T> implements InputPort<T>
  {
    InputPort<T> inputPort;

    public void set(InputPort<T> port)
    {
      inputPort = port;
    }

    public InputPort<T> get()
    {
      return inputPort;
    }

    @Override
    public void setup(PortContext context)
    {
      if (inputPort != null) {
        inputPort.setup(context);
      }
    }

    @Override
    public void teardown()
    {
      if (inputPort != null) {
        inputPort.teardown();
      }
    }

    @Override
    public Sink<T> getSink()
    {
      return inputPort == null ? null : inputPort.getSink();
    }

    @Override
    public void setConnected(boolean connected)
    {
      if (inputPort != null) {
        inputPort.setConnected(connected);
      }
    }

    @Override
    public StreamCodec<T> getStreamCodec()
    {
      return inputPort == null ? null : inputPort.getStreamCodec();
    }
  }

  /**
   * Similar to ProxyInputPort, but on output side.
   *
   * @param <T> datatype emitted on the port.
   */
  final class ProxyOutputPort<T> implements OutputPort<T>
  {
    OutputPort<T> outputPort;

    public void set(OutputPort<T> port)
    {
      outputPort = port;
    }

    public OutputPort<T> get()
    {
      return outputPort;
    }

    @Override
    public void setup(PortContext context)
    {
      if (outputPort != null) {
        outputPort.setup(context);
      }
    }

    @Override
    public void teardown()
    {
      if (outputPort != null) {
        outputPort.teardown();
      }
    }

    @Override
    public void setSink(Sink<Object> s)
    {
      if (outputPort != null) {
        outputPort.setSink(s);
      }
    }

    @Override
    public Unifier<T> getUnifier()
    {
      return outputPort == null ? null : outputPort.getUnifier();
    }
  }
}


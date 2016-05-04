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
package com.datatorrent.stram.plan.logical;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.GenericOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.Port;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.experimental.AppData;
import com.datatorrent.stram.ComponentContextPair;

/**
 * Utilities for dealing with {@link Operator} instances.
 *
 * @since 0.3.2
 */
public abstract class Operators
{
  public interface OperatorDescriptor
  {
    void addInputPort(Operator.InputPort<?> port, Field field, InputPortFieldAnnotation portAnnotation, AppData.QueryPort adqAnnotation);

    void addOutputPort(Operator.OutputPort<?> port, Field field, OutputPortFieldAnnotation portAnnotation, AppData.ResultPort adrAnnotation);
  }

  public static class PortContextPair<PORT extends Port> extends ComponentContextPair<PORT, PortContext>
  {
    public PortContextPair(PORT port, PortContext context)
    {
      super(port, context);
    }

    public PortContextPair(PORT port)
    {
      super(port, null);
    }
  }

  public static class PortMappingDescriptor implements OperatorDescriptor
  {
    public final LinkedHashMap<String, PortContextPair<InputPort<?>>> inputPorts = new LinkedHashMap<>();
    public final LinkedHashMap<String, PortContextPair<OutputPort<?>>> outputPorts = new LinkedHashMap<>();

    @Override
    public void addInputPort(Operator.InputPort<?> port, Field field, InputPortFieldAnnotation portAnnotation, AppData.QueryPort adqAnnotation)
    {
      if (!inputPorts.containsKey(field.getName())) {
        inputPorts.put(field.getName(), new PortContextPair<InputPort<?>>(port));
      }
    }

    @Override
    public void addOutputPort(Operator.OutputPort<?> port, Field field, OutputPortFieldAnnotation portAnnotation, AppData.ResultPort adrAnnotation)
    {
      if (!outputPorts.containsKey(field.getName())) {
        outputPorts.put(field.getName(), new PortContextPair<OutputPort<?>>(port));
      }
    }
  }

  public static void describe(GenericOperator operator, OperatorDescriptor descriptor)
  {
    for (Class<?> c = operator.getClass(); c != Object.class; c = c.getSuperclass()) {
      Field[] fields = c.getDeclaredFields();
      for (Field field : fields) {
        field.setAccessible(true);
        InputPortFieldAnnotation inputAnnotation = field.getAnnotation(InputPortFieldAnnotation.class);
        OutputPortFieldAnnotation outputAnnotation = field.getAnnotation(OutputPortFieldAnnotation.class);
        AppData.QueryPort adqAnnotation = field.getAnnotation(AppData.QueryPort.class);
        AppData.ResultPort adrAnnotation = field.getAnnotation(AppData.ResultPort.class);

        try {
          Object portObject = field.get(operator);

          if (portObject instanceof InputPort) {
            descriptor.addInputPort((Operator.InputPort<?>)portObject, field, inputAnnotation, adqAnnotation);
          } else {
            if (inputAnnotation != null) {
              throw new IllegalArgumentException("port is not of type " + InputPort.class.getName() + ": " + field);
            }
          }

          if (portObject instanceof OutputPort) {
            descriptor.addOutputPort((Operator.OutputPort<?>)portObject, field, outputAnnotation, adrAnnotation);
          } else {
            if (outputAnnotation != null) {
              throw new IllegalArgumentException("port is not of type " + OutputPort.class.getName() + ": " + field);
            }
          }
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}

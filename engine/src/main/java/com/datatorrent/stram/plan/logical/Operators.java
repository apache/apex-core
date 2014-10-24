/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan.logical;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Operator.Port;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

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
    public void addInputPort(Operator.InputPort<?> port, Field field, InputPortFieldAnnotation a);

    public void addOutputPort(Operator.OutputPort<?> port, Field field, OutputPortFieldAnnotation a);
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
    final public LinkedHashMap<String, PortContextPair<InputPort<?>>> inputPorts = new LinkedHashMap<String, PortContextPair<InputPort<?>>>();
    final public LinkedHashMap<String, PortContextPair<OutputPort<?>>> outputPorts = new LinkedHashMap<String, PortContextPair<OutputPort<?>>>();

    @Override
    public void addInputPort(Operator.InputPort<?> port, Field field, InputPortFieldAnnotation a)
    {
      inputPorts.put(field.getName(), new PortContextPair<InputPort<?>>(port));
    }

    @Override
    public void addOutputPort(Operator.OutputPort<?> port, Field field, OutputPortFieldAnnotation a)
    {
      outputPorts.put(field.getName(), new PortContextPair<OutputPort<?>>(port));
    }
  };

  public static void describe(Operator operator, OperatorDescriptor descriptor)
  {
    for (Class<?> c = operator.getClass(); c != Object.class; c = c.getSuperclass())
    {
      Field[] fields = c.getDeclaredFields();
      for (Field field: fields) {
        field.setAccessible(true);
        InputPortFieldAnnotation inputAnnotation = field.getAnnotation(InputPortFieldAnnotation.class);
        OutputPortFieldAnnotation outputAnnotation = field.getAnnotation(OutputPortFieldAnnotation.class);

        try {
          Object portObject = field.get(operator);

          if (portObject instanceof InputPort) {
            descriptor.addInputPort((Operator.InputPort<?>) portObject, field, inputAnnotation);
          } else {
            if (inputAnnotation != null) {
              throw new IllegalArgumentException("port is not of type " + InputPort.class.getName() + ": " + field);
            }
          }

          if (portObject instanceof OutputPort) {
            descriptor.addOutputPort((Operator.OutputPort<?>) portObject, field, outputAnnotation);
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

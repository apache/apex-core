/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan.logical;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;

import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;

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

  public static class PortMappingDescriptor implements OperatorDescriptor
  {
    final public LinkedHashMap<String, Operator.InputPort<?>> inputPorts = new LinkedHashMap<String, Operator.InputPort<?>>();
    final public LinkedHashMap<String, Operator.OutputPort<?>> outputPorts = new LinkedHashMap<String, Operator.OutputPort<?>>();

    @Override
    public void addInputPort(Operator.InputPort<?> port, Field field, InputPortFieldAnnotation a)
    {
      String portName = (a == null || a.name() == null) ? field.getName() : a.name();
      inputPorts.put(portName, port);
    }

    @Override
    public void addOutputPort(Operator.OutputPort<?> port, Field field, OutputPortFieldAnnotation a)
    {
      String portName = (a == null || a.name() == null) ? field.getName() : a.name();
      outputPorts.put(portName, port);
    }
  };

  public static void describe(Operator operator, OperatorDescriptor descriptor)
  {
    for (Class<?> c = operator.getClass(); c != Object.class; c = c.getSuperclass())
    {
      Field[] fields = c.getDeclaredFields();
      for (int i = 0; i < fields.length; i++) {
        Field field = fields[i];
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

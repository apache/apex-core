/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.api;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;

/**
 * Utilities for dealing with {@link Operator} instances.
 */
public class Operators {

  public interface OperatorDescriptor {
    public void addInputPort(Operator.InputPort<?> port, Field field, InputPortFieldAnnotation a);
    public void addOutputPort(Operator.OutputPort<?> port, Field field, OutputPortFieldAnnotation a);
  }

  public static class PortMappingDescriptor implements OperatorDescriptor {
    final public LinkedHashMap<String, Operator.InputPort<?>> inputPorts = new LinkedHashMap<String, Operator.InputPort<?>>();
    final public LinkedHashMap<String, Operator.OutputPort<?>> outputPorts = new LinkedHashMap<String, Operator.OutputPort<?>>();

    @Override
    public void addInputPort(Operator.InputPort<?> port,Field field, InputPortFieldAnnotation a)
    {
      String portName = (a.name() != null) ? a.name() : field.getName();
      inputPorts.put(portName, port);
    }

    @Override
    public void addOutputPort(Operator.OutputPort<?> port, Field field, OutputPortFieldAnnotation a)
    {
      String portName = (a.name() != null) ? a.name() : field.getName();
      outputPorts.put(portName, port);
    }
  };

  public static void describe(Operator operator, OperatorDescriptor descriptor) {
    Field[] fields = operator.getClass().getDeclaredFields();
    for (int i = 0; i < fields.length; i++) {
      Field field = fields[i];
      InputPortFieldAnnotation ia = field.getAnnotation(InputPortFieldAnnotation.class);
      if (ia != null) {
        field.setAccessible(true);
        try {
          Object portObject = field.get(operator);
          if (portObject == null) {
            throw new IllegalArgumentException("port is null " + field);
          }
          if (!(portObject instanceof Operator.InputPort)) {
            throw new IllegalArgumentException("port is not of type " + Operator.InputPort.class.getName());
          }
          descriptor.addInputPort((Operator.InputPort<?>)portObject, field, ia);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }

      OutputPortFieldAnnotation oa = field.getAnnotation(OutputPortFieldAnnotation.class);
      if (oa != null) {
        field.setAccessible(true);
        try {
          Object portObject = field.get(operator);
          if (portObject == null) {
            throw new IllegalArgumentException("port is null " + field);
          }
          if (!(portObject instanceof Operator.OutputPort)) {
            throw new IllegalArgumentException("port is not of type " + DefaultOutputPort.class.getName());
          }
          descriptor.addOutputPort((Operator.OutputPort<?>)portObject, field, oa);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}

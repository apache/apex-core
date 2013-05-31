package com.malhartech.engine;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.Unifier;

/**
 * Default unifier passes through all tuples received. Used when an operator has
 * multiple partitions and no unifier was provided through
 */
public class DefaultUnifier extends BaseOperator implements Unifier<Object>
{
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>(this);

  @Override
  public void process(Object tuple)
  {
    outputPort.emit(tuple);
  }

  private static final Logger logger = LoggerFactory.getLogger(DefaultUnifier.class);
}

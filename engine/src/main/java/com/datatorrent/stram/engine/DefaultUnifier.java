package com.datatorrent.stram.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;

/**
 * Default unifier passes through all tuples received. Used when an operator has
 * multiple partitions and no unifier was provided through
 *
 * @since 0.3.2
 */
@Stateless
public class DefaultUnifier extends BaseOperator implements Unifier<Object>
{
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>();

  @Override
  public void process(Object tuple)
  {
    outputPort.emit(tuple);
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(DefaultUnifier.class);
  private static final long serialVersionUID = 201404141917L;
}

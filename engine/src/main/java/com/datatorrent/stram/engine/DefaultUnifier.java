package com.datatorrent.stram.engine;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.Stateless;

/**
 * Default unifier passes through all tuples received. Used when an operator has
 * multiple partitions and no unifier was provided through
 *
 * @since 0.3.2
 */
@Stateless
public class DefaultUnifier implements Unifier<Object>, Serializable
{
  final public transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>();

  @Override
  public void process(Object tuple)
  {
    outputPort.emit(tuple);
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(DefaultUnifier.class);
  private static final long serialVersionUID = 201404141917L;
}

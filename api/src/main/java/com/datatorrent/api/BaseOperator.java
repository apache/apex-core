/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.api;

import com.datatorrent.api.Context.OperatorContext;

/**
 * Base class for operator implementations that provides empty implementations
 * for all interface methods.
 */
public class BaseOperator implements Operator
{
  private String name;

  public String getName() {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  @Override
  public void setup(OperatorContext context)
  {
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
  public void teardown()
  {
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{name=" + name + '}';
  }
}

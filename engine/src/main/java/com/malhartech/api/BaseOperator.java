/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.api;

import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;

/**
 * Base class for operator implementations that provides empty implementations
 * for all interface methods.
 */
public class BaseOperator implements Operator {

  private String name;

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void beginWindow()
  {
  }

  public void endWindow()
  {
  }

  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{name=" + name + '}';
  }

}

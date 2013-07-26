/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  /**
   * <p>Getter for the field <code>name</code>.</p>
   */
  public String getName() {
    return name;
  }

  /**
   * <p>Setter for the field <code>name</code>.</p>
   */
  public void setName(String name)
  {
    this.name = name;
  }

  /** {@inheritDoc} */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /** {@inheritDoc} */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /** {@inheritDoc} */
  @Override
  public void endWindow()
  {
  }

  /** {@inheritDoc} */
  @Override
  public void teardown()
  {
  }

  /** {@inheritDoc} */
  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{name=" + name + '}';
  }
}

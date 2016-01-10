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
package com.datatorrent.common.util;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;

/**
 * Base class for operator implementations that provides empty implementations
 * for all interface methods.
 *
 * @since 0.3.2
 */
public class BaseOperator implements Operator
{
  private String name;

  /**
   * @return the name property of the operator.
   */
  @Deprecated
  public String getName()
  {
    return name;
  }

  /**
   * Set the name property of the operator.
   *
   * @param name
   */
  @Deprecated
  public void setName(String name)
  {
    this.name = name;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void endWindow()
  {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void teardown()
  {
  }

  /**
   * Request orderly teardown of this operator.
   * This method communicates to the engine that this operator has finished
   * its task and wishes to shutdown. Upon receiving the shutdown request,
   * the engine would start orderly shutdown by calling endWindow for the
   * currently running window if any and subsequently call teardown on this
   * operator before claiming back resources allocated to it from JVM.
   */
  public static void shutdown()
  {
    throw new ShutdownException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{name=" + name + '}';
  }

  private static final long serialVersionUID = 201505211624L;
}

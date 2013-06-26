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

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator.Unifier;

/**
 * Output ports are declared as annotated typed fields by the operator. The
 * operator processing logic simply calls emit on the port object. Output ports
 * also define how output from replicated operators is merged.
 *
 * @param <T> - Type of the object emitted by this port.
 */
public class DefaultOutputPort<T> implements Operator.OutputPort<T>
{
  private transient Sink<Object> sink;

  public DefaultOutputPort()
  {
    this.sink = Sink.BLACKHOLE;
  }

  /**
   * Emit the given object as a payload for downstream operators interested in this port.
   *
   * @param tuple payload which needs to be emitted.
   */
  public void emit(T tuple)
  {
    sink.put(tuple);
  }

  /**
   * Called by execution engine to inject sink at deployment time.
   *
   * @param s
   */
  @Override
  final public void setSink(Sink<Object> s)
  {
    this.sink = s == null? Sink.BLACKHOLE: s;
  }

  /**
   * Opportunity for user code to check whether the port is connected, if
   * optional.
   *
   * @return true when connected, false otherwise.
   */
  public boolean isConnected()
  {
    return sink != Sink.BLACKHOLE;
  }

  /**
   * Module developer can override for getUnifier functionality
   *
   * @return Unifier<T>
   */
  @Override
  public Unifier<T> getUnifier()
  {
    return null;
  }

  @Override
  public void setup(PortContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

}

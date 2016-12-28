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
package com.datatorrent.stram.engine;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;

/**
 * <p>UnifierNode class.</p>
 *
 * @since 0.3.2
 */
public class UnifierNode extends GenericNode
{
  final Unifier<Object> unifier;

  class UnifiedPort implements InputPort<Object>, Sink<Object>
  {
    private int count;

    @Override
    public Sink<Object> getSink()
    {
      return this;
    }

    @Override
    public void setConnected(boolean connected)
    {
    }

    @Override
    public StreamCodec<Object> getStreamCodec()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public final void put(Object tuple)
    {
      count++;
      unifier.process(tuple);
    }

    @Override
    public int getCount(boolean reset)
    {
      try {
        return count;
      } finally {
        if (reset) {
          count = 0;
        }
      }
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

  final UnifiedPort unifiedPort = new UnifiedPort();

  public UnifierNode(Unifier<Object> unifier, OperatorContext context)
  {
    super(unifier, context);
    this.unifier = unifier;
  }

  @Override
  public InputPort<Object> getInputPort(String port)
  {
    descriptor.inputPorts.put(port, new PortContextPair<InputPort<?>>(unifiedPort));
    return unifiedPort;
  }

  private static final Logger logger = LoggerFactory.getLogger(UnifierNode.class);
}

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
import com.datatorrent.api.Operator.InputPort;

/**
 * Default abstract implementation for input ports.
 * An operator would typically define a derived inner class with the process method.
 * This class is designed for use with a transient field, i.e. not to be serialized with the operator state.
 *
 * @param <T>
 * @since 0.3.2
 */
public abstract class DefaultInputPort<T> implements InputPort<T>, Sink<T>
{
  private int count;
  protected boolean connected = false;

  public DefaultInputPort()
  {
  }

  /** {@inheritDoc} */
  @Override
  public Sink<T> getSink()
  {
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void setConnected(boolean connected)
  {
    this.connected = connected;
  }

  /** {@inheritDoc} */
  @Override
  public Class<? extends StreamCodec<T>> getStreamCodec()
  {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void put(T tuple)
  {
    count++;
    process(tuple);
  }

  /** {@inheritDoc} */
  @Override
  public int getCount(boolean reset)
  {
    try {
      return count;
    }
    finally {
      if (reset) {
        count = 0;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void setup(PortContext context)
  {
  }

  /** {@inheritDoc} */
  @Override
  public void teardown()
  {
  }

  public abstract void process(T tuple);

}

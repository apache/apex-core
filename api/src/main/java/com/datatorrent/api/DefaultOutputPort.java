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
package com.datatorrent.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator.Unifier;

/**
 * Output ports are declared as annotated typed fields by the operator. The
 * operator processing logic simply calls emit on the port object. Output ports
 * also define how output from replicated operators is merged.
 *
 * @param <T> - Type of the object emitted by this port.
 * @since 0.3.2
 */
public class DefaultOutputPort<T> implements Operator.OutputPort<T>
{
  public static final String THREAD_AFFINITY_DISABLE_CHECK = "com.datatorrent.api.DefaultOutputPort.thread.check.disable";
  private static final Logger logger = LoggerFactory.getLogger(DefaultOutputPort.class);

  private transient Sink<Object> sink;
  private transient Thread operatorThread;

  /**
   * <p>Constructor for DefaultOutputPort.</p>
   */
  public DefaultOutputPort()
  {
    this.sink = ControlTupleEnabledSink.BLACKHOLE;
  }

  /**
   * Emit the given object as a payload for downstream operators interested in this port.
   *
   * @param tuple payload which needs to be emitted.
   */
  public void emit(T tuple)
  {
    verifyOperatorThread();
    sink.put(tuple);
  }

  protected void verifyOperatorThread()
  {
    // operatorThread could be null if setup() never got called.
    if (operatorThread != null && Thread.currentThread() != operatorThread) {
      // only under certain modes: enforce this
      throw new IllegalStateException("Current thread " + Thread.currentThread().getName() +
        " is different from the operator thread " + operatorThread.getName());
    }
  }

  /**
   * {@inheritDoc}
   *
   * Called by execution engine to inject sink at deployment time.
   */
  @Override
  public void setSink(Sink<Object> s)
  {
    this.sink = s == null ? Sink.BLACKHOLE : s;
  }

  /**
   * Opportunity for user code to check whether the port is connected, if
   * optional.
   *
   * @return true when connected, false otherwise.
   */
  public boolean isConnected()
  {
    return sink != ControlTupleEnabledSink.BLACKHOLE;
  }

  /**
   * {@inheritDoc}
   *
   * Module developer can override for getUnifier functionality
   */
  @Override
  public Unifier<T> getUnifier()
  {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void setup(PortContext context)
  {
    if (Boolean.getBoolean(THREAD_AFFINITY_DISABLE_CHECK) == false) {
      operatorThread = Thread.currentThread();
      logger.debug("Enforcing emit on {}", operatorThread.getName());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void teardown()
  {
  }

  protected Sink<Object> getSink()
  {
    return sink;
  }
}

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
package org.apache.apex.api;

import org.apache.apex.api.operator.ControlTuple;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.ControlTupleEnabledSink;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Sink;

/**
 * Default implementation for an output port which can emit a @{@link ControlTuple}.
 * The {@link #emitControl(ControlTuple)} method can be used to emit control tuples onto this output port
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class ControlAwareDefaultOutputPort<T> extends DefaultOutputPort<T>
{
  public ControlAwareDefaultOutputPort()
  {
    setSink(ControlTupleEnabledSink.BLACKHOLE);
  }

  /**
   * Allows the operator to emit a @{@link ControlTuple}
   * @param {@link UserDefinedControlTuple}
   */
  public void emitControl(ControlTuple tuple)
  {
    verifyOperatorThread();
    ((ControlTupleEnabledSink)getSink()).putControl(tuple);
  }

  public boolean isConnected()
  {
    return getSink() != ControlTupleEnabledSink.BLACKHOLE;
  }

  @Override
  public void setSink(Sink<Object> s)
  {
    super.setSink(s == null ? ControlTupleEnabledSink.BLACKHOLE : s);
  }

}

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

import com.datatorrent.api.Component;
import com.datatorrent.api.ControlTupleEnabledSink;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Sink;

/**
 *
 * Base interface for all streams in the streaming platform<p>
 * <br>
 *
 * @since 0.3.2
 */
/*
 * Provides basic interface for a stream object. Stram, StramChild work via this interface
 */
public interface Stream extends Component<StreamContext>, ActivationListener<StreamContext>, ControlTupleEnabledSink<Object>
{
  interface MultiSinkCapableStream extends Stream
  {
    void setSink(String id, Sink<Object> sink);
  }

}

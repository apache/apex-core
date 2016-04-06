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
package com.datatorrent.stram.api;

import java.util.EnumMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StatsListener.OperatorRequest;

import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.engine.Node;

/**
 * <p>RequestFactory class.</p>
 *
 * @since 0.3.5
 */
public class RequestFactory
{
  private final EnumMap<StramToNodeRequest.RequestType, RequestDelegate> map;

  public RequestFactory()
  {
    this.map = new EnumMap<>(StramToNodeRequest.RequestType.class);
  }

  public interface RequestDelegate
  {
    OperatorRequest getRequestExecutor(final Node<?> node, final StramToNodeRequest snr);
  }

  public void registerDelegate(StramToNodeRequest.RequestType requestType, RequestDelegate delegate)
  {
    RequestDelegate old = map.put(requestType, delegate);
    if (old != null) {
      logger.warn("Replacing delegate {} for {} by {}", new Object[] {old, requestType, delegate});
    }
  }

  /**
   * Process request from stram for further communication through the protocol. Extended reporting is on a per node basis (won't occur under regular operation)
   *
   * @param node - Node which will be handling this request.
   * @param snr - The serialized request which contains context for the request.
   * @return - The actual object which will handle the request.
   */
  public OperatorRequest getRequestExecutor(final Node<?> node, final StramToNodeRequest snr)
  {
    RequestDelegate delegate = map.get(snr.requestType);
    if (delegate == null) {
      if (snr.cmd != null) {
        return snr.cmd;
      }
      return null;
    }

    return delegate.getRequestExecutor(node, snr);
  }

  private static final Logger logger = LoggerFactory.getLogger(RequestFactory.class);
}

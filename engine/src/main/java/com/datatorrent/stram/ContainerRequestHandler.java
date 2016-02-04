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
package com.datatorrent.stram;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;

/**
 * Handles creating container requests and reissuing them on timeout
 *
 */
public class ContainerRequestHandler
{
  protected static final int NUMBER_MISSED_HEARTBEATS = 30;

  public void reissueContainerRequests(AMRMClient<ContainerRequest> amRmClient, Map<StreamingContainerAgent.ContainerStartRequest, MutablePair<Integer, ContainerRequest>> requestedResources, int loopCounter, ResourceRequestHandler resourceRequestor, List<ContainerRequest> containerRequests, List<ContainerRequest> removedContainerRequests)
  {
    if (!requestedResources.isEmpty()) {
      // resourceRequestor.clearNodeMapping();
      for (Map.Entry<StreamingContainerAgent.ContainerStartRequest, MutablePair<Integer, ContainerRequest>> entry : requestedResources.entrySet()) {
        if ((loopCounter - entry.getValue().getKey()) > NUMBER_MISSED_HEARTBEATS) {
          StreamingContainerAgent.ContainerStartRequest csr = entry.getKey();
          removedContainerRequests.add(entry.getValue().getRight());
          ContainerRequest cr = resourceRequestor.createContainerRequest(csr, false);
          entry.getValue().setLeft(loopCounter);
          entry.getValue().setRight(cr);
          containerRequests.add(cr);
        }
      }
    }
  }

  public void addContainerRequest(Map<StreamingContainerAgent.ContainerStartRequest, MutablePair<Integer, ContainerRequest>> requestedResources, int loopCounter, List<ContainerRequest> containerRequests, StreamingContainerAgent.ContainerStartRequest csr, ContainerRequest cr)
  {
    MutablePair<Integer, ContainerRequest> pair = new MutablePair<Integer, ContainerRequest>(loopCounter, cr);
    requestedResources.put(csr, pair);
    containerRequests.add(cr);
  }
}

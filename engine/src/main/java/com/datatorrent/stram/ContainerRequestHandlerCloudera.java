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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.StreamingContainerAgent.ContainerStartRequest;

/**
 * Handles creating container requests and issuing node-specific container
 * requests by blacklisting specifically for cloudera
 */
public class ContainerRequestHandlerCloudera extends ContainerRequestHandler
{
  HashMap<ContainerRequest, ContainerStartRequest> hostSpecificRequests = new HashMap<>();
  HashMap<ContainerRequest, ContainerStartRequest> otherContainerRequests = new HashMap<>();
  HashMap<String, List<ContainerRequest>> hostSpecificRequestsMap = new HashMap<>();
  List<String> blacklistedNodesForHostSpecificRequests = null;

  public void reissueContainerRequests(AMRMClient<ContainerRequest> amRmClient, Map<StreamingContainerAgent.ContainerStartRequest, MutablePair<Integer, ContainerRequest>> requestedResources, int loopCounter, ResourceRequestHandler resourceRequestor, List<ContainerRequest> containerRequests, List<ContainerRequest> removedContainerRequests)
  {
    // Issue all host specific requests first
    if (!hostSpecificRequestsMap.isEmpty()) {
      LOG.info("Issue Host specific requests first");
      // Blacklist all the nodes and issue request for host specific
      Entry<String, List<ContainerRequest>> set = hostSpecificRequestsMap.entrySet().iterator().next();
      List<ContainerRequest> requests = set.getValue();
      List<String> blacklistNodes = resourceRequestor.getNodesExceptHost(requests.get(0).getNodes());
      amRmClient.updateBlacklist(blacklistNodes, requests.get(0).getNodes());
      blacklistedNodesForHostSpecificRequests = blacklistNodes;
      LOG.info("Sending {} request(s) after blacklisting nodes {} and removed host from request {}", requests.size(), blacklistNodes, requests.get(0).getNodes());

      for (ContainerRequest cr : requests) {
        ContainerStartRequest csr = hostSpecificRequests.get(cr);
        ContainerRequest newCr = new ContainerRequest(cr.getCapability(), null, null, cr.getPriority());
        MutablePair<Integer, ContainerRequest> pair = new MutablePair<Integer, ContainerRequest>(loopCounter, newCr);
        requestedResources.put(csr, pair);
        containerRequests.add(newCr);
        hostSpecificRequests.remove(cr);
      }
      hostSpecificRequestsMap.remove(set.getKey());
    } else {
      if (blacklistedNodesForHostSpecificRequests != null) {
        // Remove the nodes blacklisted nodes for host specific requests
        LOG.info("All requests done.. Removing nodes from blacklist {}", blacklistedNodesForHostSpecificRequests);
        amRmClient.updateBlacklist(null, blacklistedNodesForHostSpecificRequests);
        blacklistedNodesForHostSpecificRequests = null;
      }
      // Proceed with other requests after host specific requests are done
      if (!otherContainerRequests.isEmpty()) {
        for (Entry<ContainerRequest, ContainerStartRequest> entry : otherContainerRequests.entrySet()) {

          ContainerRequest cr = entry.getKey();
          ContainerStartRequest csr = entry.getValue();
          MutablePair<Integer, ContainerRequest> pair = new MutablePair<Integer, ContainerRequest>(loopCounter, cr);
          requestedResources.put(csr, pair);
          containerRequests.add(cr);
        }
      } else if (!otherContainerRequests.isEmpty()) {
        // Check if any requests timed out, create new requests in that case
        for (Map.Entry<StreamingContainerAgent.ContainerStartRequest, MutablePair<Integer, ContainerRequest>> entry : requestedResources.entrySet()) {
          if ((loopCounter - entry.getValue().getKey()) > NUMBER_MISSED_HEARTBEATS) {
            StreamingContainerAgent.ContainerStartRequest csr = entry.getKey();
            removedContainerRequests.add(entry.getValue().getRight());
            ContainerRequest cr = resourceRequestor.createContainerRequest(csr, false);
            if (cr.getNodes() != null && !cr.getNodes().isEmpty()) {
              addHostSpecificRequest(csr, cr);
            } else {
              otherContainerRequests.put(cr, csr);
            }
          }
        }
      }
    }
  }

  public void addContainerRequest(Map<StreamingContainerAgent.ContainerStartRequest, MutablePair<Integer, ContainerRequest>> requestedResources, int loopCounter, List<ContainerRequest> containerRequests, StreamingContainerAgent.ContainerStartRequest csr, ContainerRequest cr)
  {
    if (cr.getNodes() != null && !cr.getNodes().isEmpty()) {
      // Put it in a Map to check if multiple requests can be combined
      addHostSpecificRequest(csr, cr);
    } else {
      LOG.info("No node specific request ", cr);
      otherContainerRequests.put(cr, csr);
    }
  }

  public void addHostSpecificRequest(StreamingContainerAgent.ContainerStartRequest csr, ContainerRequest cr)
  {
    String hostKey = StringUtils.join(cr.getNodes(), ":");
    List<ContainerRequest> requests;
    if (hostSpecificRequestsMap.containsKey(hostKey)) {
      requests = hostSpecificRequestsMap.get(hostKey);
    } else {
      requests = new ArrayList<>();
    }
    requests.add(cr);
    hostSpecificRequestsMap.put(hostKey, requests);
    LOG.info("Requesting container for node {} request = {}", cr.getNodes(), cr);
    hostSpecificRequests.put(cr, csr);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ContainerRequestHandlerCloudera.class);
}

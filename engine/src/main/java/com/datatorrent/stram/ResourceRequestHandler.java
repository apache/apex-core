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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.stram.StreamingContainerAgent.ContainerStartRequest;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PTOperator.HostOperatorSet;

/**
 * Handle mapping from physical plan locality groupings to resource allocation requests. Monitors available resources
 * through node reports.
 *
 * @since 0.3.4
 */
public class ResourceRequestHandler
{

  private final static Logger LOG = LoggerFactory.getLogger(ResourceRequestHandler.class);

  public ResourceRequestHandler()
  {
    super();
  }

  /**
   * Setup the request(s) that will be sent to the RM for the container ask.
   */
  public ContainerRequest createContainerRequest(ContainerStartRequest csr, boolean first)
  {
    int priority = csr.container.getResourceRequestPriority();
    // check for node locality constraint
    String[] nodes = null;
    String[] racks = null;

    String host = getHost(csr, first);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(csr.container.getRequiredMemoryMB());
    capability.setVirtualCores(csr.container.getRequiredVCores());

    if (host != null) {
      nodes = new String[]{host};
      // in order to request a host, we don't have to set the rack if the locality is false
      /*
       * if(this.nodeToRack.get(host) != null){ racks = new String[] { this.nodeToRack.get(host) }; }
       */
      return new ContainerRequest(capability, nodes, racks, Priority.newInstance(priority), false);
    }
    // For now, only memory is supported so we set memory requirements
    return new ContainerRequest(capability, nodes, racks, Priority.newInstance(priority));
  }

  private final Map<String, NodeReport> nodeReportMap = Maps.newHashMap();
  private final Map<Set<PTOperator>, String> nodeLocalMapping = Maps.newHashMap();
  private final Map<String, String> nodeToRack = Maps.newHashMap();
  private final Map<PTContainer, String> antiAffinityMapping = Maps.newHashMap();

  public void clearNodeMapping()
  {
    nodeLocalMapping.clear();
  }

  /**
   * Tracks update to available resources. Resource availability is used to make decisions about where to request new
   * containers.
   *
   * @param nodeReports
   */
  public void updateNodeReports(List<NodeReport> nodeReports)
  {
    // LOG.debug("Got {} updated node reports.", nodeReports.size());
    for (NodeReport nr : nodeReports) {
      StringBuilder sb = new StringBuilder();
      sb.append("rackName=").append(nr.getRackName()).append(",nodeid=").append(nr.getNodeId()).append(",numContainers=").append(nr.getNumContainers()).append(",capability=").append(nr.getCapability()).append("used=").append(nr.getUsed()).append("state=").append(nr.getNodeState());
      LOG.info("Node report: " + sb);
      nodeReportMap.put(nr.getNodeId().getHost(), nr);
      nodeToRack.put(nr.getNodeId().getHost(), nr.getRackName());
    }
  }

  public List<String> getNodesExceptHost(List<String> hostNames)
  {
    List<String> nodesList = new ArrayList<String>();

    for (String host : nodeReportMap.keySet()) {
      // Split node name and port
      String[] parts = host.split(":");
      if (parts.length > 0) {
        if (hostNames.contains(parts[0]) || hostNames.contains(host)) {
          continue;
        }
        nodesList.add(parts[0]);
      }
    }
    return nodesList;
  }

  public String getHost(ContainerStartRequest csr, boolean first)
  {
    String host = null;
    PTContainer c = csr.container;
    if (first) {
      for (PTOperator oper : c.getOperators()) {
        HostOperatorSet grpObj = oper.getNodeLocalOperators();
        host = nodeLocalMapping.get(grpObj.getOperatorSet());
        if (host != null) {
          antiAffinityMapping.put(c, host);
          return host;
        }
        if (grpObj.getHost() != null) {
          host = grpObj.getHost();
          // using the 1st host value as host for container
          break;
        }
      }
      if (host != null && nodeReportMap.get(host) != null) {
        for (PTOperator oper : c.getOperators()) {
          HostOperatorSet grpObj = oper.getNodeLocalOperators();
          Set<PTOperator> nodeLocalSet = grpObj.getOperatorSet();
          NodeReport report = nodeReportMap.get(host);
          int aggrMemory = c.getRequiredMemoryMB();
          int vCores = c.getRequiredVCores();
          Set<PTContainer> containers = Sets.newHashSet();
          containers.add(c);
          for (PTOperator nodeLocalOper : nodeLocalSet) {
            if (!containers.contains(nodeLocalOper.getContainer())) {
              aggrMemory += nodeLocalOper.getContainer().getRequiredMemoryMB();
              vCores += nodeLocalOper.getContainer().getRequiredVCores();
              containers.add(nodeLocalOper.getContainer());
            }
          }
          int memAvailable = report.getCapability().getMemory() - report.getUsed().getMemory();
          int vCoresAvailable = report.getCapability().getVirtualCores() - report.getUsed().getVirtualCores();
          if (memAvailable >= aggrMemory && vCoresAvailable >= vCores) {
            nodeLocalMapping.put(nodeLocalSet, host);
            antiAffinityMapping.put(c, host);
            return host;
          }
        }
      }
    }

    // the host requested didn't have the resources so looking for other hosts
    host = null;
    List<String> antiHosts = new ArrayList<>();
    List<String> antiPreferredHosts = new ArrayList<>();
    if (c.getStrictAntiPrefs() != null && !c.getStrictAntiPrefs().isEmpty()) {
      // Check if containers are allocated already for the anti-affinity
      // containers
      populateAntiHostList(c, antiHosts);
    }
    if (c.getPreferredAntiPrefs() != null && !c.getPreferredAntiPrefs().isEmpty()) {
      populateAntiHostList(c, antiPreferredHosts);
    }
    LOG.info("Strict anti-affinity = {}", antiHosts);
    for (PTOperator oper : c.getOperators()) {
      HostOperatorSet grpObj = oper.getNodeLocalOperators();
      Set<PTOperator> nodeLocalSet = grpObj.getOperatorSet();
      if (nodeLocalSet.size() > 1 || (c.getStrictAntiPrefs() != null)) {
        LOG.info("Finding new host for {}", nodeLocalSet);
        int aggrMemory = c.getRequiredMemoryMB();
        int vCores = c.getRequiredVCores();
        Set<PTContainer> containers = Sets.newHashSet();
        containers.add(c);
        // aggregate memory required for all containers
        for (PTOperator nodeLocalOper : nodeLocalSet) {
          if (!containers.contains(nodeLocalOper.getContainer())) {
            aggrMemory += nodeLocalOper.getContainer().getRequiredMemoryMB();
            vCores += nodeLocalOper.getContainer().getRequiredVCores();
            containers.add(nodeLocalOper.getContainer());
          }
        }
        host = assignHost(host, antiHosts, antiPreferredHosts, grpObj, nodeLocalSet, aggrMemory, vCores);

        if (host == null && !antiPreferredHosts.isEmpty()) {
          // Drop the preferred constraint and try allocation
          antiPreferredHosts.clear();
          host = assignHost(host, antiHosts, antiPreferredHosts, grpObj, nodeLocalSet, aggrMemory, vCores);
        }
      }
    }
    if (host != null) {
      antiAffinityMapping.put(c, host);
    }
    LOG.info("Found host {}", host);
    return host;
  }

  public void populateAntiHostList(PTContainer c, List<String> antiHosts)
  {
    for (PTContainer container : c.getStrictAntiPrefs()) {
      if (antiAffinityMapping.containsKey(container)) {
        antiHosts.add(antiAffinityMapping.get(container));
      } else {
        // Check if there is an anti-affinity with host locality
        String antiHost = getAntiHostsList(container);
        if (antiHost != null) {
          antiHosts.add(antiHost);
        }
      }
    }
  }

  public String getAntiHostsList(PTContainer container)
  {
    for (PTOperator oper : container.getOperators()) {
      HostOperatorSet grpObj = oper.getNodeLocalOperators();
      String host = nodeLocalMapping.get(grpObj.getOperatorSet());
      if (host != null) {
        return host;
      }
      if (grpObj.getHost() != null) {
        host = grpObj.getHost();
        return host;
      }
    }
    return null;
  }

  public String assignHost(String host, List<String> antiHosts, List<String> antiPreferredHosts, HostOperatorSet grpObj, Set<PTOperator> nodeLocalSet, int aggrMemory, int vCores)
  {
    for (Map.Entry<String, NodeReport> nodeEntry : nodeReportMap.entrySet()) {
      int memAvailable = nodeEntry.getValue().getCapability().getMemory() - nodeEntry.getValue().getUsed().getMemory();
      int vCoresAvailable = nodeEntry.getValue().getCapability().getVirtualCores() - nodeEntry.getValue().getUsed().getVirtualCores();
      if (memAvailable >= aggrMemory && vCoresAvailable >= vCores && !antiHosts.contains(nodeEntry.getKey()) && !antiPreferredHosts.contains(nodeEntry.getKey())) {
        host = nodeEntry.getKey();
        grpObj.setHost(host);
        nodeLocalMapping.put(nodeLocalSet, host);

        return host;
      }
    }
    return null;
  }

}

/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.util.Records;

import com.datatorrent.stram.StramChildAgent.ContainerStartRequest;
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
  public ContainerRequest createContainerRequest(ContainerStartRequest csr, int memory, boolean first)
  {
    int priority = csr.container.getResourceRequestPriority();
    // check for node locality constraint
    String[] nodes = null;
    String[] racks = null;

    String host = getHost(csr, memory, first);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);

    if (host != null) {
      nodes = new String[] { host };
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

  public String getHost(ContainerStartRequest csr, int requiredMemory, boolean first)
  {
    String host = null;
    PTContainer c = csr.container;
    if (first) {            
      for (PTOperator oper : c.getOperators()) {
        HostOperatorSet grpObj = oper.getNodeLocalOperators();
        host = nodeLocalMapping.get(grpObj.getOperatorSet());
        if (host != null)
          return host;
        if (grpObj.getHost() != null) {
          host = grpObj.getHost();
          // using the 1st host value as host for container
          break;
        }
      }
      if (host != null) {
        for (PTOperator oper : c.getOperators()) {
          HostOperatorSet grpObj = oper.getNodeLocalOperators();
          Set<PTOperator> nodeLocalSet = grpObj.getOperatorSet();
          NodeReport report = nodeReportMap.get(host); // report null means the host passed is wrong
          if (report != null) {
            int aggrMemory = 0;
            Set<PTContainer> containers = Sets.newHashSet();
            for (PTOperator nodeLocalOper : nodeLocalSet) {
              if (!containers.contains(nodeLocalOper.getContainer())) {
                aggrMemory += requiredMemory;
                containers.add(nodeLocalOper.getContainer());
              }
            }
            int memAvailable = report.getCapability().getMemory() - report.getUsed().getMemory();
            if (memAvailable >= aggrMemory) {
              nodeLocalMapping.put(nodeLocalSet, host);
              return host;
            }
          }
        }
      }
    }
    // the host requested didn't have the resources so looking for other hosts
    host = null;
    for (PTOperator oper : c.getOperators()) {
      HostOperatorSet grpObj = oper.getNodeLocalOperators();
      Set<PTOperator> nodeLocalSet = grpObj.getOperatorSet();
      if (nodeLocalSet.size() > 1) {
        LOG.debug("Finding new host for {}", nodeLocalSet);
        int aggrMemory = 0;
        Set<PTContainer> containers = Sets.newHashSet();
        // aggregate memory required for all containers
        for (PTOperator nodeLocalOper : nodeLocalSet) {
          if (!containers.contains(nodeLocalOper.getContainer())) {
            aggrMemory += requiredMemory;
            containers.add(nodeLocalOper.getContainer());
          }
        }
        for (Map.Entry<String, NodeReport> nodeEntry : nodeReportMap.entrySet()) {
          int memAvailable = nodeEntry.getValue().getCapability().getMemory() - nodeEntry.getValue().getUsed().getMemory();
          if (memAvailable >= aggrMemory) {
            host = nodeEntry.getKey();
            nodeLocalMapping.put(nodeLocalSet, host);
            return host;
          }
        }
      }
    }
    return host;
  }

}

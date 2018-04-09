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
package org.apache.apex.engine.events.grouping;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.events.grouping.GroupingRequest.EventGroupId;

import com.google.common.collect.Maps;

import com.datatorrent.stram.plan.physical.PTOperator;

/**
 * This class manages tracking ids of deploy/undeploy for containers and
 * operators.
 *
 *
 * @since 3.7.0
 */
public class GroupingManager
{
  private static final GroupingManager groupingManager = new GroupingManager();
  private Map<String, GroupingRequest> groupingRequests = Maps.newHashMap();

  public static GroupingManager getGroupingManagerInstance()
  {
    return groupingManager;
  }

  /**
   * Retruns all available grouping requests with StrAM
   * @return groupingRequests
   */
  public Map<String, GroupingRequest> getGroupingRequests()
  {
    return groupingRequests;
  }

  /**
   * Returns grouping request for container
   * @param containerId
   * @return groupingRequest
   */
  public GroupingRequest getGroupingRequest(String containerId)
  {
    if (containerId != null) {
      return groupingRequests.get(containerId);
    }
    return null;
  }

  /**
   * Returns deploy/undeploy group Id for container
   * @param containerId
   * @return groupId <br/>
   *         <b>Note:</b> groupId 0 indicates and independent event, with no
   *         group
   */
  public EventGroupId getEventGroupIdForContainer(String containerId)
  {
    EventGroupId groupId = null;
    if (containerId != null && groupingRequests.get(containerId) != null) {
      groupId = groupingRequests.get(containerId).getEventGroupId();
    }
    return groupId;
  }

  /**
   * Returns deploy/undeploy group Id for container This could be a new
   * container allocated during redeploy process
   * @param containerId
   * @return groupId <br/>
   *         <b>Note:</b> groupId 0 indicates and indipendent event, with no
   *         group
   */
  public EventGroupId getEventGroupIdForAffectedContainer(String containerId)
  {
    EventGroupId groupId = getEventGroupIdForContainer(containerId);
    if (groupId != null) {
      return groupId;
    }
    for (GroupingRequest request : getGroupingRequests().values()) {
      if (request.getAffectedContainers().contains(containerId)) {
        groupId = request.getEventGroupId();
      }
    }
    return groupId;
  }

  /**
   * Returns grouping groupId for operator which is to undergo deploy. Operators
   * undergoing deploy for first time will have groupId as 0
   * @param operatorId
   * @return groupId <br/>
   *         <b>Note:</b> groupId 0 indicates and indipendent event, with no
   *         group
   */
  public EventGroupId getEventGroupIdForOperatorToDeploy(int operatorId)
  {
    for (GroupingRequest request : getGroupingRequests().values()) {
      if (request.getOperatorsToDeploy().contains(operatorId)) {
        return request.getEventGroupId();
      }
    }
    return null;
  }

  /**
   * Adds operator to deploy. The operator is added to request associated with containerId
   * @param containerIs
   * @param operator
   */
  public void addOperatorToDeploy(String containerId, PTOperator oper)
  {
    GroupingRequest request = getGroupingRequest(containerId);
    if (request != null) {
      request.addOperatorToDeploy(oper.getId());
    }
  }

  /**
   * Removes operator from grouping request
   */
  public boolean removeOperatorFromGroupingRequest(int operatorId)
  {
    for (GroupingRequest request : getGroupingRequests().values()) {
      if (request.getOperatorsToDeploy().contains((operatorId))) {
        return request.removeOperatorToDeploy(operatorId);
      }
    }
    return false;
  }

  /**
   * Remove groupingRequest from StrAM if it has no more pending operators to deploy
   * @param containerId
   */
  public void removeProcessedGroupingRequests()
  {
    Iterator<Entry<String, GroupingRequest>> itr = groupingRequests.entrySet().iterator();
    while (itr.hasNext()) {
      Entry<String, GroupingRequest> entry = itr.next();
      if (entry.getValue().isProcessed()) {
        LOG.debug("Removing Grouping request for : {}", entry.getKey());
        itr.remove();
      }
    }
  }

  /**
   * Create groupingRequest to group deploy/undeploy of related container/operator
   * events under one groupId to find related events.
   * To start will all related operators are added to opertorsToUndeploy list,
   * they will eventually move to operatorsToDeploy when operator undergo redeploy cycle.
   * @param containerId
   * @param affectedOperators
   */
  public GroupingRequest addOrModifyGroupingRequest(String containerId, Set<PTOperator> affectedOperators)
  {
    GroupingRequest request = groupingRequests.get(containerId);
    if (request == null) {
      request = new GroupingRequest();
      groupingRequests.put(containerId, request);
    }
    for (PTOperator oper : affectedOperators) {
      request.addOperatorToUndeploy(oper.getId());
      request.addAffectedContainer(oper.getContainer().getExternalId());
    }
    return request;
  }

  /**
   * Add affectedContainerId to deploy request, if container is deployed as part
   * of redeploy process of groupLeaderContainer
   * @param groupLeaderContainerId
   * @param affectedContainerId
   */
  public void addNewContainerToGroupingRequest(String groupLeaderContainerId, String affectedContainerId)
  {
    if (groupLeaderContainerId != null && affectedContainerId != null) {
      GroupingRequest request = getGroupingRequest(groupLeaderContainerId);
      if (request != null) {
        request.addAffectedContainer(affectedContainerId);
      }
    }
  }

  /**
   * When operator state changes from PENDING_UNDEPLOY to PENDING_DEPLOY move
   * operator from operatorsToUndeploy to operatorsToDeploy
   * @param operator
   * @return groupId
   */
  public EventGroupId moveOperatorFromUndeployListToDeployList(PTOperator oper)
  {
    EventGroupId groupId = null;
    for (GroupingRequest request : groupingRequests.values()) {
      if (request.getOperatorsToUndeploy().contains(oper.getId())) {
        groupId = request.getEventGroupId();
        request.removeOperatorToUndeploy(oper.getId());
        request.addOperatorToDeploy(oper.getId());
      }
    }
    return groupId;
  }

  /**
   * Clear all grouping requests
   */
  public void clearAllGroupingRequests()
  {
    groupingRequests.clear();
  }

  private static final Logger LOG = LoggerFactory.getLogger(GroupingManager.class);
}

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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;

import com.datatorrent.stram.util.AbstractWritableAdapter;

/**
 * Grouping request keeps track of operators whose start/stop events should be grouped.
 *
 * @since 3.7.0
 */
public class GroupingRequest
{
  private EventGroupId eventGroupId;
  private Set<Integer> operatorsToDeploy = Sets.newHashSet();
  private Set<Integer> operatorsToUndeploy = Sets.newHashSet();
  private Set<String> affectedContainers = Sets.newHashSet();

  public GroupingRequest()
  {
    eventGroupId = EventGroupId.newEventGroupId();
  }

  public GroupingRequest(EventGroupId groupId)
  {
    this.eventGroupId = groupId;
  }

  /**
   * Gets EventGroupId
   * @return eventGroupId
   */
  public EventGroupId getEventGroupId()
  {
    return eventGroupId;
  }

  /**
   * Gets operators to deploy as part of deploy request
   * @return operatorsToDeploy
   */
  public Set<Integer> getOperatorsToDeploy()
  {
    return operatorsToDeploy;
  }

  /**
   * Gets operators to undeploy as part of deploy request
   * @return operatorsToUndeploy
   */
  public Set<Integer> getOperatorsToUndeploy()
  {
    return operatorsToUndeploy;
  }

  /**
   * Gets containers affected by deploy request
   * @return affectedContainers
   */
  public Set<String> getAffectedContainers()
  {
    return affectedContainers;
  }

  /**
   * Adds operator to deploy request's list of operators to deploy
   * @param operatorId
   */
  public void addOperatorToDeploy(int operatorId)
  {
    operatorsToDeploy.add(operatorId);
  }

  /**
   * Removes operator from deploy request's list of operators to deploy
   * @param operatorId
   * @return ifRemoved
   */
  public boolean removeOperatorToDeploy(int operatorId)
  {
    return operatorsToDeploy.remove(operatorId);
  }

  /**
   * Adds operator to deploy request's list of operators to undeploy
   * @param operatorId
   */
  public void addOperatorToUndeploy(int operatorId)
  {
    operatorsToUndeploy.add(operatorId);
  }

  /**
   * Removes operator from deploy request's list of operators to undeploy
   * @param operatorId
   * @return ifRemoved
   */
  public boolean removeOperatorToUndeploy(int operatorId)
  {
    return operatorsToUndeploy.remove(operatorId);
  }

  /**
   * Adds container to deploy request's list of affected containers.
   * @param containerId
   */
  public void addAffectedContainer(String containerId)
  {
    affectedContainers.add(containerId);
  }

  /**
   * Checks if request is processed
   * @return isProcessed
   */
  public boolean isProcessed()
  {
    return (getOperatorsToDeploy().isEmpty() && getOperatorsToUndeploy().isEmpty());
  }

  /**
   * EventGroupId is used to club relevant events. Events triggered by common
   * cause are considered as relevant events.
   *
   */
  public static class EventGroupId extends AbstractWritableAdapter
  {
    private static final long serialVersionUID = 1L;
    private static final AtomicInteger idSequence = new AtomicInteger();
    private int groupId;

    public static EventGroupId newEventGroupId()
    {
      EventGroupId id = new EventGroupId();
      id.groupId = idSequence.incrementAndGet();
      return id;
    }

    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + groupId;
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      EventGroupId other = (EventGroupId)obj;
      if (groupId != other.groupId) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
      return "EventGroupId [groupId=" + groupId + "]";
    }

  }
}

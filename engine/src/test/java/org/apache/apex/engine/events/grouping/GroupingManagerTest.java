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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableSet;

import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;

import static org.mockito.Mockito.when;

public class GroupingManagerTest
{

  @Mock
  private PTOperator oper1;
  @Mock
  private PTOperator oper2;
  @Mock
  private PTContainer testContainer;
  private String affectedContainerId = "container_4";
  private GroupingManager underTest;

  @Before
  public void setup()
  {
    underTest = GroupingManager.getGroupingManagerInstance();
    MockitoAnnotations.initMocks(this);

    when(oper1.getId()).thenReturn(1);
    when(oper2.getId()).thenReturn(2);
    when(oper1.getContainer()).thenReturn(testContainer);
    when(oper2.getContainer()).thenReturn(testContainer);
    when(testContainer.getExternalId()).thenReturn(affectedContainerId);
  }

  @Test
  public void testAddNewDeploy()
  {
    String failedContainerId = "container_1";
    underTest.addOrModifyGroupingRequest(failedContainerId, ImmutableSet.of(oper1, oper2));
    Assert.assertEquals(1, underTest.getGroupingRequests().size());
    GroupingRequest request = underTest.getGroupingRequest(failedContainerId);
    Assert.assertTrue(request.getAffectedContainers().contains(affectedContainerId));
    Assert.assertTrue(request.getOperatorsToUndeploy().contains(oper1.getId()));
    Assert.assertTrue(request.getOperatorsToUndeploy().contains(oper2.getId()));
  }

  @Test
  public void testAddOperatorToGroupingRequest()
  {
    String failedContainerId = "container_1";
    underTest.addOrModifyGroupingRequest(failedContainerId, ImmutableSet.of(oper1));
    GroupingRequest request = underTest.getGroupingRequest(failedContainerId);
    Assert.assertFalse(request.getOperatorsToDeploy().contains(oper2.getId()));
    underTest.addOperatorToDeploy(failedContainerId, oper2);
    Assert.assertTrue(request.getOperatorsToDeploy().contains(oper2.getId()));
  }

  @Test
  public void testGetDeployGroupIdForContainer()
  {
    String failedContainerId = "container_1";
    underTest.addOrModifyGroupingRequest(failedContainerId, ImmutableSet.of(oper1));
    GroupingRequest request = underTest.getGroupingRequest(failedContainerId);

    Assert.assertEquals(request.getEventGroupId(), underTest.getEventGroupIdForContainer(failedContainerId));
  }

  @Test
  public void testGetDeployGroupIdForOperator()
  {
    String failedContainerId = "container_1";
    underTest.addOrModifyGroupingRequest(failedContainerId, ImmutableSet.of(oper1));
    underTest.addOperatorToDeploy(failedContainerId, oper1); //consider operator moved from PENDING_UNDEPLOY to DENDING_DEPLOY state
    GroupingRequest request = underTest.getGroupingRequest(failedContainerId);

    Assert.assertEquals(request.getEventGroupId(), underTest.getEventGroupIdForOperatorToDeploy(oper1.getId()));
  }

  @Test
  public void testMoveOperatorFromUndeployListToDeployList()
  {
    String failedContainerId = "container_1";
    underTest.addOrModifyGroupingRequest(failedContainerId, ImmutableSet.of(oper1));
    underTest.moveOperatorFromUndeployListToDeployList(oper1);
    GroupingRequest request = underTest.getGroupingRequest(failedContainerId);

    Assert.assertFalse(request.getOperatorsToUndeploy().contains(oper1.getId()));
    Assert.assertTrue(request.getOperatorsToDeploy().contains(oper1.getId()));
  }

  @Test
  public void testAddNewContainerToGroupingRequest()
  {
    String groupLeaderContainerId = "container_1";
    String newAffectedContainerId = "container_11";
    underTest.addOrModifyGroupingRequest(groupLeaderContainerId, ImmutableSet.of(oper1));
    underTest.addNewContainerToGroupingRequest(groupLeaderContainerId, newAffectedContainerId);

    GroupingRequest request = underTest.getGroupingRequest(groupLeaderContainerId);
    Assert.assertTrue(request.getAffectedContainers().contains(newAffectedContainerId));
  }

  @Test
  public void testRemoveProcessedGroupingRequest()
  {
    underTest.addOrModifyGroupingRequest(affectedContainerId, ImmutableSet.of(oper1));
    Assert.assertEquals(1, underTest.getGroupingRequests().size());
    underTest.moveOperatorFromUndeployListToDeployList(oper1); //move from updeploy to deploy list
    underTest.removeOperatorFromGroupingRequest(oper1.getId());
    underTest.removeProcessedGroupingRequests();
    Assert.assertEquals(0, underTest.getGroupingRequests().size());

  }

  @After
  public void teardown()
  {
    underTest.clearAllGroupingRequests();
  }
}

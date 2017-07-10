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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GroupingRequestTest
{
  private GroupingRequest underTest;

  @Before
  public void setup()
  {
    underTest = new GroupingRequest();
  }

  @Test
  public void testAddAffectedContainer()
  {
    String affectedContainerId = "container_000001";
    underTest.addAffectedContainer(affectedContainerId);
    Assert.assertTrue(underTest.getAffectedContainers().contains(affectedContainerId));
  }

  @Test
  public void testAddOperatorToUndeploy()
  {
    int operatorId = 1;
    underTest.addOperatorToUndeploy(operatorId);
    Assert.assertTrue(underTest.getOperatorsToUndeploy().contains(operatorId));
  }

  @Test
  public void testAddOperatorToDeploy()
  {
    int operatorId = 1;
    underTest.addOperatorToDeploy(operatorId);
    Assert.assertTrue(underTest.getOperatorsToDeploy().contains(operatorId));
  }

  @Test
  public void testRemoveOperatorToUndeploy()
  {
    int operatorId = 1;
    underTest.addOperatorToUndeploy(operatorId);
    Assert.assertTrue(underTest.getOperatorsToUndeploy().contains(operatorId));
    underTest.removeOperatorToUndeploy(operatorId);
    Assert.assertFalse(underTest.getOperatorsToUndeploy().contains(operatorId));
  }

  @Test
  public void testRemoveOperatorToDeploy()
  {
    int operatorId = 1;
    underTest.addOperatorToDeploy(operatorId);
    Assert.assertTrue(underTest.getOperatorsToDeploy().contains(operatorId));
    underTest.removeOperatorToDeploy(operatorId);
    Assert.assertFalse(underTest.getOperatorsToDeploy().contains(operatorId));
  }

}

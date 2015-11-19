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
package com.datatorrent.bufferserver.policy;

import java.util.Set;

import com.datatorrent.bufferserver.internal.PhysicalNode;
import com.datatorrent.bufferserver.util.SerializedData;

/**
 *
 * Implements load balancing by sending the tuple to the least busy partition.
 * Basic load balancing policy. Extends the base class {@link AbstractPolicy}<br>
 *
 * @since 0.3.2
 */
public class LeastBusy extends AbstractPolicy
{
  static final LeastBusy instance = new LeastBusy();

  /**
   *
   * @return {@link com.datatorrent.bufferserver.policy.LeastBusy}
   */
  public static LeastBusy getInstance()
  {
    return instance;
  }

  /**
   * Constructor
   */
  private LeastBusy()
  {
  }

  @Override
  public boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException
  {
    PhysicalNode theOne = null;

    for (PhysicalNode node: nodes) {
      if (theOne == null || node.getProcessedMessageCount() < theOne.getProcessedMessageCount()) {
        theOne = node;
      }
    }

    return theOne == null ? false : theOne.send(data);
  }

}

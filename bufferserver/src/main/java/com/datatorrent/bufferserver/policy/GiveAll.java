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
 * Implements policy of giving a tuple to all nodes<p>
 * <br>
 * Is a broadcast policy. Extends the base class {@link com.datatorrent.bufferserver.policy.AbstractPolicy}<br>
 *
 * @since 0.3.2
 */
public class GiveAll extends AbstractPolicy
{
  private static final GiveAll instance = new GiveAll();

  /**
   *
   * @return {@link com.datatorrent.bufferserver.policy.GiveAll}
   */
  public static GiveAll getInstance()
  {
    return instance;
  }

  /**
   *
   *
   * @param nodes Set of downstream {@link com.datatorrent.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.datatorrent.bufferserver.util.SerializedData} to be send
   * @return true if blocked, false otherwise
   * @throws InterruptedException
   */
  @Override
  public boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException
  {
    boolean retval = true;
    for (PhysicalNode node: nodes) {
      retval = node.send(data) & retval;
    }

    return retval;
  }

}

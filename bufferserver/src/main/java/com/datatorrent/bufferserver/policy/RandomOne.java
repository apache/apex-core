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
 * Randomly distributes tuples to downstream nodes. A random load balancing policy.
 * A generic random load balancing policy. Extends the base class {@link AbstractPolicy}
 *
 * @since 0.3.2
 */
public class RandomOne extends AbstractPolicy
{
  static final RandomOne instance = new RandomOne();

  /**
   *
   * @return {@link com.datatorrent.bufferserver.policy.RandomOne}
   */
  public static RandomOne getInstance()
  {
    return instance;
  }

  /**
   * Constructor
   */
  private RandomOne()
  {
  }

  @Override
  public boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException
  {
    int count = (int)(Math.random() * nodes.size());
    /*
     * Should look at accessing nodes within the Set as array. Will save iteration through all the
     * physical nodes.
     *
     */
    for (PhysicalNode node : nodes) {
      if (count-- == 0) {
        return node.send(data);
      }
    }

    return false;
  }

}

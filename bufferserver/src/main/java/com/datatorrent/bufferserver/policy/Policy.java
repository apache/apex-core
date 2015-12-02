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
 * The base interface for implementing/specifying partition policies<p>
 * <br>
 *
 * @since 0.3.2
 */
public interface Policy
{
  /**
   * Distributes {@code data} to the set of {@code nodes}
   *
   * @param nodes Set of downstream {@link PhysicalNode}
   * @param data Opaque {@link SerializedData} to be send
   * @throws InterruptedException
   * @return {@code true} if successful, otherwise {@code false}
   */
  boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException;

}

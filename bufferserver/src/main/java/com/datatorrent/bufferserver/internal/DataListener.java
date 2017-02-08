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
package com.datatorrent.bufferserver.internal;

import java.util.Collection;

import com.datatorrent.bufferserver.util.BitVector;

/**
 *
 * Waits for data to be added to the buffer server and then acts on it<p>
 * <br>
 * The behavior upon data addition is customizable
 *
 * @since 0.3.2
 */
public interface DataListener
{
  BitVector NULL_PARTITION = new BitVector(0, 0);

  /**
   */
  boolean addedData(boolean checkIfListenerHaveDataToSendOnly);

  /**
   *
   * @param partitions
   * @return int
   */
  int getPartitions(Collection<BitVector> partitions);

}

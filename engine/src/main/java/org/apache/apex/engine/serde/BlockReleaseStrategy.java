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
package org.apache.apex.engine.serde;

/**
 * The process of interface would be:
 * - Stream keep on reporting how many free blocks it has in certain frequent. usually at the end of each window
 * - Stream check how many block should release. Stream usually release the blocks but Stream can make its own decision
 * - Stream report how many blocks actually released
 */
public interface BlockReleaseStrategy
{
  /**
   * The stream should call this method to report to the strategy how many blocks are free currently.
   * @param freeBlockNum
   */
  void currentFreeBlocks(int freeBlockNum);

  /**
   * Get how many blocks can be released
   * @return
   */
  int getNumBlocksToRelease();

  /**
   * The stream should call this method to report how many block are released.
   * @param numReleasedBlocks
   */
  void releasedBlocks(int numReleasedBlocks);

}

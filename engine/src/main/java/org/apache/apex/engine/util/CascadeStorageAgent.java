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
package org.apache.apex.engine.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.common.util.AsyncStorageAgent;

import com.google.common.collect.Maps;

import com.datatorrent.api.StorageAgent;

/**
 * A StorageAgent which chains two StorageAgent. It use the current storage-agent to store
 * the checkpoint, and use the parent agent to read old checkpoints. For application having
 * large number of physical operators, the size and number of files to be copied could be
 * large impacting application restart time. This storage-agent is used during application
 * restart to avoiding copying checkpoints from old application directory to improve application
 * restart time.
 *
 * @since 3.6.0
 */
public class CascadeStorageAgent implements StorageAgent, AsyncStorageAgent, Serializable
{
  private static final long serialVersionUID = 985557590735264920L;
  private static final Logger logger = LoggerFactory.getLogger(CascadeStorageAgent.class);
  private final StorageAgent parent;
  private final StorageAgent current;
  private transient Map<Integer, long[]> oldOperatorToWindowIdsMap;

  public CascadeStorageAgent(StorageAgent parent, StorageAgent current)
  {
    this.parent = parent;
    this.current = current;
    oldOperatorToWindowIdsMap = Maps.newConcurrentMap();
  }

  /**
   * does the checkpoint belong to parent
   */
  private boolean isCheckpointFromParent(int operatorId, long wid) throws IOException
  {
    long[] wids = getParentWindowIds(operatorId);
    if (wids.length != 0) {
      return (wid <= wids[wids.length - 1]);
    }
    return false;
  }

  /**
   * Return window-id of checkpoints available in old storage agent. This function
   * will call getWindowIds of old storage agent only once for the fist time, and
   * return cached data for next calls for same operator.
   *
   * @param operatorId
   * @return
   * @throws IOException
   */
  private long[] getParentWindowIds(int operatorId) throws IOException
  {
    long[] oldWindowIds = oldOperatorToWindowIdsMap.get(operatorId);
    if (oldWindowIds == null) {
      oldWindowIds = parent.getWindowIds(operatorId);
      if (oldWindowIds == null) {
        oldWindowIds = new long[0];
      }
      Arrays.sort(oldWindowIds);
      oldOperatorToWindowIdsMap.put(operatorId, oldWindowIds);
      logger.debug("CascadeStorageAgent window ids from old storage agent op {} wids {}", operatorId, Arrays.toString(oldWindowIds));
    }
    return oldWindowIds;
  }

  /**
   * Save object in current storage agent. This should not modify old storage agent
   * in any way.
   *
   * @param object - The operator whose state needs to be saved.
   * @param operatorId - Identifier of the operator.
   * @param windowId - Identifier for the specific state of the operator.
   * @throws IOException
   */
  @Override
  public void save(Object object, int operatorId, long windowId) throws IOException
  {
    current.save(object, operatorId, windowId);
  }

  /**
   * Delete old checkpoints from the storage agent.
   *
   * The checkpoints are deleted from current directory if it is present in current
   * storage agent. and cached state for old storage agent is removed.
   *
   * @param operatorId
   * @param windowId
   * @throws IOException
   */
  @Override
  public void delete(int operatorId, long windowId) throws IOException
  {
    if (!isCheckpointFromParent(operatorId, windowId)) {
      current.delete(operatorId, windowId);
    }
  }

  /**
   * Load checkpoint from storage agents. Do a basic comparision of windowIds
   * to check the storage agent which has the checkpoint.
   *
   * @param operatorId Id for which the object was previously saved
   * @param windowId WindowId for which the object was previously saved
   * @return
   * @throws IOException
   */
  @Override
  public Object load(int operatorId, long windowId) throws IOException
  {
    long[] oldWindowIds = getParentWindowIds(operatorId);
    if (oldWindowIds.length >= 1 && windowId <= oldWindowIds[oldWindowIds.length - 1]) {
      return parent.load(operatorId, windowId);
    }
    return current.load(operatorId, windowId);
  }

  @Override
  public long[] getWindowIds(int operatorId) throws IOException
  {
    long[] currentIds = current.getWindowIds(operatorId);
    long[] oldWindowIds = getParentWindowIds(operatorId);
    return merge(currentIds, oldWindowIds);
  }

  private static final long[] EMPTY_LONG_ARRAY = new long[0];
  private long[] merge(long[] currentIds, long[] oldWindowIds)
  {
    if (currentIds == null && oldWindowIds == null) {
      return EMPTY_LONG_ARRAY;
    }
    if (currentIds == null) {
      return oldWindowIds;
    }
    if (oldWindowIds == null) {
      return currentIds;
    }
    long[] mergedArray = new long[currentIds.length + oldWindowIds.length];
    System.arraycopy(currentIds, 0, mergedArray, 0, currentIds.length);
    System.arraycopy(oldWindowIds, 0, mergedArray, currentIds.length, oldWindowIds.length);
    Arrays.sort(mergedArray);
    return mergedArray;
  }

  @Override
  public void flush(int operatorId, long windowId) throws IOException
  {
    if (current instanceof AsyncStorageAgent) {
      ((AsyncStorageAgent)current).flush(operatorId, windowId);
    }
  }

  @Override
  public boolean isSyncCheckpoint()
  {
    if (parent instanceof AsyncStorageAgent) {
      return ((AsyncStorageAgent)parent).isSyncCheckpoint();
    }
    return true;
  }

  private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException
  {
    input.defaultReadObject();
    oldOperatorToWindowIdsMap = Maps.newConcurrentMap();
  }

  public StorageAgent getCurrentStorageAgent()
  {
    return current;
  }

  public StorageAgent getParentStorageAgent()
  {
    return parent;
  }
}

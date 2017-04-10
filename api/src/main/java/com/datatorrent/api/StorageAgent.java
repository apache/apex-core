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
package com.datatorrent.api;

import java.io.IOException;

import com.datatorrent.api.Attribute.AttributeMap;

/**
 * Interface to define writing/reading checkpoint state of any operator.
 *
 * @since 0.9.4
 */
public interface StorageAgent
{
  /**
   * Save the object so that the same object can be loaded later using the given combination of
   * operatorId and windowId.
   *
   * Typically the storage agent would serialize the state of the object during the save state.
   * The serialized state can be accessed from anywhere on the cluster to recreate the object
   * through the load callback.
   *
   * @param object - The operator whose state needs to be saved.
   * @param operatorId - Identifier of the operator.
   * @param windowId - Identifier for the specific state of the operator.
   * @throws IOException
   */
  void save(Object object, int operatorId, long windowId) throws IOException;

  /**
   * Get the input stream from which can be used to retrieve the stored objects back.
   *
   * @param operatorId Id for which the object was previously saved
   * @param windowId WindowId for which the object was previously saved
   * @return object (or a copy of it) which was saved earlier using the save call.
   * @throws IOException
   */
  Object load(int operatorId, long windowId) throws IOException;

  /**
   * Delete the artifacts related to store call of the operatorId and the windowId.
   *
   * Through this call, the agent is informed that the object saved against the operatorId
   * and the windowId together will not be needed again.
   *
   * @param operatorId
   * @param windowId
   * @throws IOException
   */
  void delete(int operatorId, long windowId) throws IOException;

  /**
   * Return an array windowId for which the object was saved but not deleted.
   *
   * The set is essentially difference between two sets. The first set contains
   * all the windowIds passed using the successful save calls. The second set contains all
   * the windowIds passed using the successful delete calls.
   *
   * @param operatorId - The operator for which the state was saved.
   * @return Collection of windowIds for available states that can be retrieved through load.
   * @throws IOException
   */
  long[] getWindowIds(int operatorId) throws IOException;

  /**
   * Interface to pass application attributes to storage agent
   *
   */
  interface ApplicationAwareStorageAgent extends StorageAgent
  {

    /**
     * Passes attributes of application to storage agent
     *
     * @param map attributes of application
     */
    void setApplicationAttributes(AttributeMap map);
  }

}

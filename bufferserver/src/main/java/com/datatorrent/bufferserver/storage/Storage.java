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
package com.datatorrent.bufferserver.storage;

import java.io.IOException;

/**
 * Interface used by the internal messaging service to temporarily store the messages.
 *
 * Streaming Platform uses a messaging service internally to transfer the data among the containers.
 * The messaging service stores the data in the memory as long as there is sufficient memory. When
 * it runs out of memory, instead of failing the associated containers, it has an option to swap the
 * data to external storage. The following simple interface abstracts such an external storage.
 *
 * The implementation of the storage needs to be threadsafe.
 *
 * @since 0.3.2
 */
public interface Storage
{
  /**
   * Get an instance of the class which implements Storage interface.
   * This method is useful if the platform decides that one instance is not enough for it to perform
   * the secondary storage operations efficiently.
   * // normalize this for a given identifier
   *
   * @return instance of storage.
   * @throws IOException
   */
  Storage getInstance() throws IOException;

  /**
   * Store memory block represented by block in non memory storage.
   * Streaming Platform calls this method to persist the data on secondary storage so the memory
   * can be freed.
   *
   * @param Identifier - application specific Id, this identifies the data source. The platform will determine the
   * value of the value of thie param. Identifier is typically the OperatorId.Portname where OperatorId is numerical
   * Id of the operator whose port named Portname is generating the data. The platform
   * the instance is free to assign it a unique id. If non zero, the instance should use that id to store the block
   * and return it.
   * @param bytes - memory represented as byte array
   * @param start - the offset of the first byte in the array
   * @param end - the offset of the last byte in the array.
   * @return unique identifier for the stored block.
   */
  int store(String Identifier, byte[] bytes, int start, int end);

  /**
   *
   * @param identifier primary identifier of the block which typically identifies the data source.
   * @param uniqueIdentifier secondary and unique identifier of the block which needs to be retrived.
   * @return memory block which was stored with the passed parameters as identifying information.
   */
  byte[] retrieve(String identifier, int uniqueIdentifier);

  /**
   * Discard the block stored from the secondary storage.
   *
   * @param identifier
   * @param uniqueIdentifier
   */
  void discard(String identifier, int uniqueIdentifier);
}

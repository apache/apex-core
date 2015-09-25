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

import com.datatorrent.netlet.util.Slice;

/**
 * Serializing and Deserializing the data tuples and controlling the partitioning
 * <p>
 * Data flows from one Operator to another Operator through a stream.Typically
 * StreamCodec is defined on each input stream and is able to
 * serialize/deserialize and partition the data of type supported by the stream.
 * <br />
 * <br />
 * For a few known types, the system is able to determine the StreamCodec. In
 * all other cases, it would need user to define the codec on each input stream.
 * <br />
 * <br />
 * In the physical layout, each codec has at least 2 instances - serializer
 * instance which attaches itself to the stream coming out of upstream operator
 * and deserializer instance which attaches itself to input stream of downstream
 * operator.
 *
 * @param <T> data type of the tuples on the stream
 * @since 0.3.2
 */
public interface StreamCodec<T>
{
  /**
   * Create POJO from the byte array for consumption by the downstream.
   *
   * @param fragment
   * @return plain old java object, the type is intentionally not T since the consumer does not care about it.
   */
  Object fromByteArray(Slice fragment);

  /**
   * Serialize the POJO emitted by the upstream node to byte array so that
   * it can be transmitted or stored in file
   *
   * @param o plain old java object
   * @return serialized representation of the object
   */
  Slice toByteArray(T o);

  /**
   * Get the partition on the object to be delivered to the downstream
   * so that it can be sent to appropriate downstream node if the load
   * balancing per partition is in effect.
   *
   * @param o object for which the partition has to be determined
   * @return partition for the object
   */
  int getPartition(T o);

}

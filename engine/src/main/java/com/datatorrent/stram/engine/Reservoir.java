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
package com.datatorrent.stram.engine;

/**
 * <p>Reservoir interface.</p>
 *
 * @since 0.3.2
 */
public interface Reservoir<T>
{
  /**
   * @param dataTupleAware if true excludes control tuples from the returned count, otherwise returns the count of all
   *     elements in the reservoir including control tuples.
   * @return number of elements including or excluding control tuples in the Reservoir. Depending on implementation
   * {@code size()} may return exact number of elements in the reservoir or an over estimate. In case {@code size()}
   * returns {@code 0} the reservoir is guaranteed to be empty, while {@code size() != 0} does not imply that
   * {@code isEmpty()} is {@code false}
   *
   * @since 3.3
   */
  int size(final boolean dataTupleAware);

  /**
   * Remove an element from the reservoir.
   *
   * @return the removed element.
   */
  T remove();

  /**
   * @return true if reservoir is empty, false otherwise
   *
   * @since 3.3
   */
  boolean isEmpty();

}

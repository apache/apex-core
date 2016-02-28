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

import com.datatorrent.api.Sink;
import com.datatorrent.stram.tuple.Tuple;

/**
 * <p>SweepableReservoir interface.</p>
 *
 * @since 0.3.2
 */
public interface SweepableReservoir extends Reservoir<Object>
{
  /**
   * Set a new sink on this reservoir where data tuples would be put.
   *
   * @param sink The new Sink for the data tuples
   * @return The old sink if present or null
   */
  Sink<Object> setSink(Sink<Object> sink);

  /**
   * Consume all the data tuples until control tuple is encountered.
   *
   * @return The control tuple encountered or null
   */
  Tuple sweep();

  /**
   * Get the count of tuples consumed.
   *
   * @param reset flag to indicate if the count should be reset to zero after this operation
   * @return the count of tuples
   */
  int getCount(boolean reset);

}

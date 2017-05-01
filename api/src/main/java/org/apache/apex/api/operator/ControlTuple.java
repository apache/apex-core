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
package org.apache.apex.api.operator;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Any user generated control tuple must implement {@link ControlTuple} interface
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public interface ControlTuple
{
  /**
   * A user generated control tuple must specify a @{@link DeliveryType}
   * @return @{@link DeliveryType} type
   */
  DeliveryType getDeliveryType();

  /**
   * All custom control tuples can be delivered according to the following semantics
   * 1. IMMEDIATE - The control tuple will be delivered immediately to the next operator
   * 2. END_WINDOW - The control tuple will be delivered to the next operator just before the
   * com.datatorrent.api.Operator#endWindow() call.
   */
  enum DeliveryType
  {
    IMMEDIATE,
    END_WINDOW
  }
}

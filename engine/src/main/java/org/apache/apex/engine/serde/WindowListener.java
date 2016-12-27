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

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Operator;

/**
 * This interface represents components which need to listen to the operator {@link Operator#beginWindow(long)} and
 * {@link Operator#endWindow()} callbacks.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface WindowListener
{
  /**
   * This is called when the parent {@link Operator}'s {@link Operator#beginWindow(long)} callback is called.
   * @param windowId The id of the current application window.
   */
  void beginWindow(long windowId);

  /**
   * This is called when the parent {@link Operator}'s {@link Operator#endWindow()} callback is called.
   */
  void endWindow();
}

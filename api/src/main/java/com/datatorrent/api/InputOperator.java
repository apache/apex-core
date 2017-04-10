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

/**
 * Input operators ie operators which do not consume payload from other operators but are able
 * to generate the payload for other operators must implement this interface.
 *
 * It's an error to provide input ports on operators which implement this interface since existence
 * of ports contradicts with the purpose of the interface.
 *
 * @since 0.3.2
 */
public interface InputOperator extends Operator
{
  /**
   * The input operators are given an opportunity to emit the tuples which will be marked
   * as belonging to the windowId identified by immediately preceding beginWindow call.
   *
   * emitTuples can implement one or more tuples. Ideally it should emit as many tuples as
   * possible without going over the streaming window width boundary. Since operators can
   * easily spend too much time making that check, when not convenient they should emit
   * only as many tuples as they can confidently within the current window. The streaming
   * engine will make sure to call emitTuples multiple times within a giving streaming
   * window if it can. When it cannot, it will call endWindow.
   */
  void emitTuples();

}

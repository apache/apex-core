/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api;

/**
 * Interface operator must implement if it's interested in being notified when it's idling.
 *
 * When the operator is idling, i.e. for GenericOperator no input is being processed or for InputOperator
 * no output is being produced, it's explicitly notified of such a state. The operators which implement
 * this interface should make use of this idle time to do any auxiliary processing they may want to do
 * when operator is idling. If the operator has no need to do such auxiliary processing, they should not
 * implement this interface. In which case, the engine will put the operator in scaled back processing mode
 * to better utilize CPU. It resumes its normal processing as soon as it detects tuples being received
 * or generated. If this interface is implemented, care should be taken to ensure that it will not result
 * in busy loop because the engine keeps calling handleIdleTime until it does not have tuples which it
 * can give to the operator.
 *
 * @since 0.3.2
 */
public interface IdleTimeHandler
{
  /**
   * Callback for operators to implement if they are interested in using the idle cycles to do auxiliary processing.
   * If this method detects that it does not have any work to do, it should block the call for a short duration
   * to prevent busy loop. handleIdleTime is called over and over until operator has tuples to process.
   */
  public void handleIdleTime();

}

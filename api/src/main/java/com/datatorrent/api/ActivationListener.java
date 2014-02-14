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
 * Interface operator must implement if they want the the engine to inform them as
 * they are activated or before they are deactivated.
 *
 * An operator may be subjected to activate/deactivate cycle multiple times during
 * its lifetime which is bounded by setup/teardown method pair. So it's advised that
 * all the operations which need to be done right before the first window is delivered
 * to the operator be done during activate and opposite be done in the deactivate.
 *
 * An example of where one would consider implementing ActivationListener is an
 * input operator which wants to consume a high throughput stream. Since there is
 * typically at least a few hundreds of milliseconds between the time the setup method
 * is called and the first window, you would want to place the code to activate the
 * stream inside activate instead of setup.
 *
 * @param <CONTEXT> Context for the current run during which the operator is getting de/activated.
 * @since 0.3.2
 */
public interface ActivationListener<CONTEXT extends Context>
{
  /**
   * Do the operations just before the operator starts processing tasks within the windows.
   * e.g. establish a network connection.
   * @param context - the context in which the operator is executing.
   */
  public void activate(CONTEXT context);

  /**
   * Do the opposite of the operations the operator did during activate.
   * e.g. close the network connection.
   */
  public void deactivate();

}

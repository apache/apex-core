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
 * @param <CONTEXT> Context for the current run during which the operator is getting de/activated.
 * @since 0.3.2
 */
public interface ActivationListener<CONTEXT extends Context>
{
  /**
   * <p>activate.</p>
   */
  public void activate(CONTEXT ctx);

  /**
   * <p>deactivate.</p>
   */
  public void deactivate();

}

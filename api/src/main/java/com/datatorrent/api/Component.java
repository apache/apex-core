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
 * Basic interface which is implemented by almost all entities in the system.
 * This interface provides a convenient way of setting up and tearing down most
 * entities in the system.
 *
 * @param <T1> Context used for the current run of the component.
 * @since 0.3.2
 */
public interface Component<T1 extends Context>
{
  /**
   * <p>setup.</p>
   */
  public void setup(T1 context);

  /**
   * <p>teardown.</p>
   */
  public void teardown();

}

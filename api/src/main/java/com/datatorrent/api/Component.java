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
   * It's recommended to use this separator to create scoped names for the components.
   * e.g. Port p on Operator o can be identified as o.concat(CONCAT_SEPARATOR).concat(p).
   */
  public static final String CONCAT_SEPARATOR = ".";
  /**
   * It's recommended to use this separator to split the scoped names into individual components.
   * e.g. o.concat(CONCAT_SEPARATOR).concat(p).split(SPLIT_SEPARATOR) will return String[]{o, p}.
   *
   */
  public static final String SPLIT_SEPARATOR = "\\.";

  /**
   * Callback to give the component a chance to perform tasks required as part of setting itself up.
   */
  public void setup(T1 context);

  /**
   * Callback to give the component a chance to perform tasks required as part of tearing itself down.
   * A recommended practice is to do reciprocate the setup tasks by doing exactly opposite.
   */
  public void teardown();

}

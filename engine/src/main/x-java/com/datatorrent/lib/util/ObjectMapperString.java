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
package com.datatorrent.lib.util;

/**
 *
 * For JSON raw serialization, assumes the passed string to be a valid javascript value representation
 *
 * @since 0.3.2
 */
public class ObjectMapperString
{
  public String string;

  /**
   * <p>Constructor for ObjectMapperString.</p>
   */
  public ObjectMapperString(String string)
  {
    this.string = string;
  }

  /** {@inheritDoc} */
  @Override
  public String toString()
  {
    return string;
  }

}

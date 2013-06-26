/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.api.util;

/**
 *
 * For JSON raw serialization, assumes the passed string to be a valid javascript value representation
 */
public class ObjectMapperString
{
  public String string;

  public ObjectMapperString(String string)
  {
    this.string = string;
  }

  @Override
  public String toString()
  {
    return string;
  }

}

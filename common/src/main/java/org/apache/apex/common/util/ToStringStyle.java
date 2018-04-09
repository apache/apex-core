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
package org.apache.apex.common.util;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

@Evolving
/**
 * @since 3.7.0
 */
public abstract class ToStringStyle extends org.apache.commons.lang.builder.ToStringStyle
{
  public static final ToStringStyle SHORT_CLASS_NAME_STYLE = new ShortClassNameToStringStyle();
  public static final ToStringStyle SIMPLE_SHORT_CLASS_NAME_STYLE = new SimpleShortClassNameToStringStyle();
  public static final ToStringStyle DEFAULT = SHORT_CLASS_NAME_STYLE;

  private ToStringStyle()
  {
    super();
  }

  private static class ShortClassNameToStringStyle extends ToStringStyle
  {
    private ShortClassNameToStringStyle()
    {
      super();
      setUseShortClassName(true);
    }
  }

  private static class SimpleShortClassNameToStringStyle extends ShortClassNameToStringStyle
  {
    private SimpleShortClassNameToStringStyle()
    {
      super();
      setFieldSeparatorAtStart(false);
      setFieldSeparatorAtEnd(false);
    }
  }
}

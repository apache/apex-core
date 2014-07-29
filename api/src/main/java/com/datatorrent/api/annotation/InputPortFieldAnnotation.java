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
package com.datatorrent.api.annotation;

import java.lang.annotation.*;
/**
 *
 * Annotation for input ports on streaming operators.<p>
 *
 * @since 0.3.2
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface InputPortFieldAnnotation
{
  /**
   * Alternative name of the port. When this parameter is not defined or set to empty string,
   * the name of the field is used.
   *
   * @return - name of the parameter.
   */
  public String name();
  /**
   * Whether this port connection is optional. When true, you may choose not to connect the port.
   *
   * @return - true if port is optional, false otherwise.
   */
  public boolean optional() default false;

}

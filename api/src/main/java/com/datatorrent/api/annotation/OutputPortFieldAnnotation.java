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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 *
 * Annotation for output ports on streaming operators.
 *
 * @since 0.3.2
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OutputPortFieldAnnotation {

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
  public boolean optional() default true;

  /**
   * Whether this port emits an error stream.
   *
   * @return - true if port is an error port, false otherwise.
   */
  public boolean error() default false;

  /**
   * Display name of the port.
   *
   * @return - port name
   */
  public String displayName() default "";

  /**
   * A short description (1-2 sentences) of this output port.
   *
   * @return port description
   */
  public String description() default "";

}

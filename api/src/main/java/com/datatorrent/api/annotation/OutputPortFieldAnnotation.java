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
package com.datatorrent.api.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * Annotation for output ports on streaming operators.<p>
 *
 * @since 0.3.2
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OutputPortFieldAnnotation
{
  /**
   * <p>optional.</p>
   */
  boolean optional() default true;
  /**
   * <p>error.</p>
   */
  boolean error() default false;

  /**
   * Whether this port needs to know the tuple class. When true, application will have to set
   * the port attribute- TUPLE_CLASS of the port otherwise dag validation will fail.
   *
   * @return  true if schema is required; false otherwise.
   */
  boolean schemaRequired() default false;
}


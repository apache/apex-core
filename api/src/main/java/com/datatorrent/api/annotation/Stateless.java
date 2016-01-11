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
 * When a class or interface is annotated with this annotation, the object which are
 * instanceof such a class or interface are assumed to have the same serialized state
 * it attains after it's configured completely.
 *
 * This annotation is typically is used to mark the operator Stateless. That means
 * the checkpointing for such an operator will be skipped. In case of failure the
 * serialized instance of the operator which was submitted to the DAG will be used to
 * reinstantiate it and used to recover the application from the assumed checkpoint
 * onwards.
 *
 * @since 0.9.4
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Stateless
{
  /**
   * WindowId of the unaltered state of an operator.
   * The first windowId during which data may be given to the operator is zero.
   */
  long WINDOW_ID = -1;
}

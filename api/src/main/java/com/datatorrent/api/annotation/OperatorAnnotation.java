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

import com.datatorrent.api.Operator;

/**
 * Annotation to specify characteristics of an operator.
 *
 * @since 0.3.5
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface OperatorAnnotation
{
  /**
   * Element specifying whether operator can be partitioned or not.
   * Default value is true indicating operator can be partitioned.
   * @return Whether operator can be partitioned or not
   */
  boolean partitionable() default true;

  /**
   * Element specifying whether an operator can be check-pointed in the middle of an application window.
   * Default value is true indicating that it can be. When false the checkpoint window count should be a multiple of
   * application window count otherwise the dag validation will fail.
   *
   * @return whether operator can be checkpointed in middle of an application window.
   */
  boolean checkpointableWithinAppWindow() default true;

  /**
   * Element specifying the recovery mode for the operator.
   * By default the operator state is recovered from checkpoint. The operator developer can indicate a preference for
   * the recovery mode with this element, see {@link Operator.RecoveryMode}. The application developer can override this
   * by setting the {@link Context.OperatorContext#RECOVERY_MODE} attribute.
   * @return
   */
  Operator.RecoveryMode recoveryMode() default Operator.RecoveryMode.CHECKPOINT;
}

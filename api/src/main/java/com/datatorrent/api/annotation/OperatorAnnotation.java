/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify characteristics of an operator.<p>
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
  public boolean partitionable() default true;
}

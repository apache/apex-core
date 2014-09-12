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
 * Annotation to specify characteristics of an operator property. Needs to
 * be attached to the field getter method.
 *
 * @since 1.0.5
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PropertyAnnotation
{
  /**
   * The name of the property to display in various tools.
   *
   * @return operator display name
   */
  public String displayName() default "";

  /**
   * A short description (1-2 sentences) of this operator.
   *
   * @return operator description
   */
  public String description() default "";

  /**
   * Property validation rules (spec TODO).
   *
   * @return validation rules spec
   */
  public String validation() default "";

}

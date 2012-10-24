/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the element value can be injected from configuration.
 * The injector will be provided a configuration accessor for the given key.
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface InjectConfig {
  /**
   * The configuration key
   * @return
   */
  public String key();

  /**
   * If true, and the configuration is not found,
   * skip injection rather than produce an error.
   */
  boolean optional() default true;
}

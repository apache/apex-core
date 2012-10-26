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
 * Indicates that the property value can be injected from configuration,
 * according to the lookup hierarchy applicable for the enclosing type. By using
 * the annotation the operator author specifies that a particular property is
 * environment specific or environment setting, if present, may override a DAG
 * defined value.
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface InjectConfig {
  /**
   * The configuration key.
   * The injector will be provided a configuration accessor for the given key.
   * @return
   */
  public String key();

  /**
   * If true, and no configuration is found,
   * skip injection rather than produce an error.
   */
  boolean optional() default true;
}

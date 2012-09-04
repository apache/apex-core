/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * 
 * Annotation definition for port types for Nodes<p>
 * 
 */

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface PortAnnotation {
  public enum PortType {
    DEAD,
    INPUT,
    OUTPUT,
    BIDI
  }

  public String name();
  public PortType type();
}

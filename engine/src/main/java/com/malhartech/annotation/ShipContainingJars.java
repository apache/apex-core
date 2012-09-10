/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 *
 * Annotation to indicate a jar file dependency that needs to be deployed to cluster when launching the application.<p>
 * <br>
 * Can be used with Node and SerDe classes that can be
 * configured in the topology.<br>
 * <br>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ShipContainingJars {
  Class<?>[] classes();
}

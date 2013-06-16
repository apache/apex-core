/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.api.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 *
 * Annotation to indicate a jar file dependency that needs to be deployed to cluster when launching the application.<p>
 * <br>
 * Can be used with {@link Operator} and {@link StreamCodec} classes that can be
 * configured in the DAG.<br>
 * <br>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ShipContainingJars {
  Class<?>[] classes();
}

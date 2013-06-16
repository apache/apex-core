/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api.annotation;

import java.lang.annotation.*;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RecordField
{
   public String type();
}

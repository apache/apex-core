/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

import com.datatorrent.api.Context.OperatorContext;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ContextInitializer
{
  public static void initialize(Class<? extends Context> clazz)
  {
    Field[] declaredFields = clazz.getDeclaredFields();
    for (Field f : declaredFields) {
      if (Modifier.isStatic(f.getModifiers())) {
        Class<?> cl = f.getType();
        if (AttributeMap.AttributeKey.class.isAssignableFrom(cl)) {
          logger.debug("{} = {}", f.getName(), f);
        }
      }
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(ContextInitializer.class);
}

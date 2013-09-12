/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.api;

import java.util.HashSet;
import java.util.Set;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public interface ContainerContext extends Context
{
  public static final AttributeKey<String> IDENTIFIER = new AttributeKey<String>("IDENTIFIER", String.class);

  public class AttributeKey<T> extends AttributeMap.AttributeKey<T>
  {
    public final Class<T> attributeType;

    @SuppressWarnings("LeakingThisInConstructor")
    private AttributeKey(String name, Class<T> type)
    {
      super(ContainerContext.class, name);
      this.attributeType = type;
      INSTANCES.add(this);
    }

    private final static Set<AttributeKey<?>> INSTANCES = new HashSet<AttributeKey<?>>();
  }
}

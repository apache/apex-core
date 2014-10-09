/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.api;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context;

import com.datatorrent.stram.util.AbstractWritableAdapter;

/**
 * <p>BaseContext class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class BaseContext extends AbstractWritableAdapter implements Context
{
  /*
   * the following 2 need to be public since otherwise they are not serialized.
   */
  public final AttributeMap attributes;
  public final Context parentContext; // may be we do not need to serialize parentContext!
  public Object counters;

  public BaseContext(AttributeMap attributes, Context parentContext)
  {
    this.attributes = attributes == null ? new DefaultAttributeMap() : attributes;
    this.parentContext = parentContext;
  }

  @Override
  public AttributeMap getAttributes()
  {
    return attributes;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    T attr = attributes.get(key);
    if (attr != null) {
      return attr;
    }
    return parentContext == null ? key.defaultValue : parentContext.getValue(key);
  }

  @Override
  public void setCounters(Object counters)
  {
    this.counters = counters;
  }

  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  private static final long serialVersionUID = 201306060103L;
}

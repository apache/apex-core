/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.api;

import com.datatorrent.stram.util.AbstractWritableAdapter;
import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.AttributeMap.AttributeKey;
import com.datatorrent.api.Context;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class BaseContext extends AbstractWritableAdapter implements Context
{
  private static final long serialVersionUID = 201306060103L;
  /*
   * the following 2 need to be public since otherwise they are not serialized.
   */
  public final AttributeMap attributes;
  public final Context parentContext;

  public BaseContext(AttributeMap attributes, Context parentContext)
  {
    this.attributes = attributes;
    this.parentContext = parentContext;
  }

  @Override
  public AttributeMap getAttributes()
  {
    return attributes;
  }

  @Override
  public <T> T attrValue(AttributeKey<T> key, T defaultValue)
  {
    Attribute<T> attr = attributes.attr(key);
    if (attr != null) {
      T get = attr.get();
      if (get != null) {
        return get;
      }
    }

    return parentContext == null? defaultValue: parentContext.attrValue(key, defaultValue);
  }

}

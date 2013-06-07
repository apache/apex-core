/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram.api;

import com.malhartech.api.AttributeMap;
import com.malhartech.api.AttributeMap.Attribute;
import com.malhartech.api.AttributeMap.AttributeKey;
import com.malhartech.api.Context;
import com.malhartech.stram.util.AbstractWritableAdapter;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class BaseContext extends AbstractWritableAdapter implements Context
{
  private static final long serialVersionUID = 201306060103L;
  /*
   * the followiing 2 need to be public since otherwise they are not serialized.
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

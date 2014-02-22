/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.stram.api.BaseContext;
import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context;

/**
 * <p>PortContext class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class PortContext extends BaseContext implements Context.PortContext
{

  public PortContext(AttributeMap attributes, Context parentContext)
  {
    super(attributes, parentContext);
  }

  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  private static final long serialVersionUID = 201306071424L;
}

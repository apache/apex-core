/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.engine;

import com.datatorrent.stram.api.BaseContext;
import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class PortContext extends BaseContext implements Context.PortContext
{
  private static final long serialVersionUID = 201306071424L;

  public PortContext(AttributeMap attributes, Context parentContext)
  {
    super(attributes, parentContext);
  }

}

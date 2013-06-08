/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.AttributeMap;
import com.malhartech.api.Context;
import com.malhartech.stram.api.BaseContext;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PortContext extends BaseContext implements Context.PortContext
{
  private static final long serialVersionUID = 201306071424L;

  public PortContext(AttributeMap attributes, Context parentContext)
  {
    super(attributes, parentContext);
  }

}

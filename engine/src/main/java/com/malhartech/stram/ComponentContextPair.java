/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.Component;
import com.malhartech.api.Context;


/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ComponentContextPair<COMPONENT extends Component<?>, CONTEXT extends Context> extends ComponentComplementPair<COMPONENT, CONTEXT>
{
  public final CONTEXT context;

  public ComponentContextPair(COMPONENT component, CONTEXT context)
  {
    super(component);
    this.context = context;
  }

  @Override
  public CONTEXT getComplement()
  {
    return context;
  }

  @Override
  public String toString()
  {
    return "ComponentContextPair{component=" + component + ", context=" + context + '}';
  }

}

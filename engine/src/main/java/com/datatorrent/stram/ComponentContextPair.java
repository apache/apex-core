/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.api.ComponentComplementPair;
import com.datatorrent.api.Component;
import com.datatorrent.api.Context;


/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
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

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;


/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ComponentContextPair<COMPONENT extends Component, CONTEXT extends Context>
{
  public final COMPONENT component;
  public final CONTEXT context;

  public ComponentContextPair(COMPONENT component, CONTEXT context)
  {
    this.component = component;
    this.context = context;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 73 * hash + (this.component != null ? this.component.hashCode() : 0);
    hash = 73 * hash + (this.context != null ? this.context.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ComponentContextPair<COMPONENT, CONTEXT> other = (ComponentContextPair<COMPONENT, CONTEXT>)obj;
    if (this.component != other.component && (this.component == null || !this.component.equals(other.component))) {
      return false;
    }
    if (this.context != other.context && (this.context == null || !this.context.equals(other.context))) {
      return false;
    }
    return true;
  }
}

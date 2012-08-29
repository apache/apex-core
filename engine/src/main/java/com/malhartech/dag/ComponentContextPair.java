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
  public COMPONENT component;
  public CONTEXT context;

  public ComponentContextPair(COMPONENT component, CONTEXT context)
  {
    this.component = component;
    this.context = context;
  }

  // check if there are compelling argument for another composite hash code routine.
  /**
   *
   * @return
   */
  @Override
  public int hashCode()
  {
    int hashCode = this.component == null? 0: (this.component.hashCode() >> 1);
    hashCode += this.context == null? 0: (this.context.hashCode() >> 1);
    return hashCode;
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
    final ComponentContextPair other = (ComponentContextPair)obj;
    if (this.component != other.component && (this.component == null || !this.component.equals(other.component))) {
      return false;
    }
    if (this.context != other.context && (this.context == null || !this.context.equals(other.context))) {
      return false;
    }
    return true;
  }
}

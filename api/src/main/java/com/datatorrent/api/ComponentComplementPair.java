/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

/**
 * A utility class to club component along with the entity such as context or configuration.
 *
 * We use ComponentComplementPair for better readability of the code compared to using a bare
 * pair where first and second do not have semantic meaning.
 *
 * @param <COMPONENT>
 * @param <COMPLEMENT>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class ComponentComplementPair<COMPONENT extends Component<?>, COMPLEMENT>
{
  public final COMPONENT component;

  public ComponentComplementPair(COMPONENT component)
  {
    this.component = component;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 73 * hash + (this.component != null ? this.component.hashCode() : 0);
    hash = 73 * hash + (this.getComplement() != null ? this.getComplement().hashCode() : 0);
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
    @SuppressWarnings("unchecked")
    final ComponentComplementPair<COMPONENT, COMPLEMENT> other = (ComponentComplementPair<COMPONENT, COMPLEMENT>)obj;
    if (this.component != other.component && (this.component == null || !this.component.equals(other.component))) {
      return false;
    }
    if (this.getComplement() != other.getComplement() && (this.getComplement() == null || !this.getComplement().equals(other.getComplement()))) {
      return false;
    }
    return true;
  }

  public abstract COMPLEMENT getComplement();

}

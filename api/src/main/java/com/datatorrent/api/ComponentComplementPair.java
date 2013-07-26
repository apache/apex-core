/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * @author Chetan Narsude <chetan@datatorrent.com>
 * 
 * @since 0.3.2
 */
public abstract class ComponentComplementPair<COMPONENT extends Component<?>, COMPLEMENT>
{
  public final COMPONENT component;

  public ComponentComplementPair(COMPONENT component)
  {
    this.component = component;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 73 * hash + (this.component != null ? this.component.hashCode() : 0);
    hash = 73 * hash + (this.getComplement() != null ? this.getComplement().hashCode() : 0);
    return hash;
  }

  /** {@inheritDoc} */
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

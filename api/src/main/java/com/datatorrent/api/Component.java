/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.api;

/**
 * Basic interface which is implemented by almost all entities in the system.
 * This interface provides a convenient way of setting up and tearing down most
 * entities in the system.
 *
 * @param <CONTEXT> Context used for the current run of the component.
 * @since 0.3.2
 */
public interface Component<CONTEXT extends Context>
{
  /**
   * It's recommended to use this separator to create scoped names for the components.
   * e.g. Port p on Operator o can be identified as o.concat(CONCAT_SEPARATOR).concat(p).
   */
  String CONCAT_SEPARATOR = ".";
  /**
   * It's recommended to use this separator to split the scoped names into individual components.
   * e.g. o.concat(CONCAT_SEPARATOR).concat(p).split(SPLIT_SEPARATOR) will return String[]{o, p}.
   *
   */
  String SPLIT_SEPARATOR = "\\.";

  /**
   * Callback to give the component a chance to perform tasks required as part of setting itself up.
   * This callback is made exactly once during the operator lifetime. If you want to perform certain
   * setup tasks every time the operator is activated/deactivated, please implement ActivationListener.
   *
   * @param context - context in which the operator executues.
   */
  void setup(CONTEXT context);

  /**
   * Callback to give the component a chance to perform tasks required as part of tearing itself down.
   * A recommended practice is to reciprocate the tasks in setup by doing exactly opposite.
   */
  void teardown();

  /**
   * A utility class to club component along with the entity such as context or configuration.
   *
   * We use ComponentComplementPair for better readability of the code compared to using a bare
   * pair where first and second do not have semantic meaning.
   *
   * @param <COMPONENT>
   * @param <COMPLEMENT>
   * @since 0.3.2
   */
  abstract class ComponentComplementPair<COMPONENT extends Component<?>, COMPLEMENT>
  {
    public final COMPONENT component;

    /**
     * <p>Constructor for ComponentComplementPair.</p>
     */
    public ComponentComplementPair(COMPONENT component)
    {
      super();
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
      @SuppressWarnings(value = "unchecked")
      final ComponentComplementPair<COMPONENT, COMPLEMENT> other = (ComponentComplementPair<COMPONENT, COMPLEMENT>)obj;
      if (this.component != other.component && (this.component == null || !this.component.equals(other.component))) {
        return false;
      }
      if (this.getComplement() != other.getComplement() && (this.getComplement() == null || !this.getComplement().equals(other.getComplement()))) {
        return false;
      }
      return true;
    }

    /**
     * <p>getComplement.</p>
     */
    public abstract COMPLEMENT getComplement();

  }

}

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
package com.datatorrent.stram;


import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Component;
import com.datatorrent.api.Component.ComponentComplementPair;
import com.datatorrent.api.Context;


/**
 * <p>ComponentConfigurationPair class.</p>
 *
 * @since 0.3.2
 */
public class ComponentConfigurationPair<COMPONENT extends Component<Context>, CONFIG extends Configuration> extends ComponentComplementPair<COMPONENT, CONFIG>
{
  public final CONFIG config;

  public ComponentConfigurationPair(COMPONENT component, CONFIG context)
  {
    super(component);
    this.config = context;
  }

  @Override
  public CONFIG getComplement()
  {
    return config;
  }
}

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
package org.apache.apex.api.plugin;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;

/**
 * DAGSetupPlugin allows user provided code to run at various stages
 * during DAG preparation. Currently following stages are supported
 *
 * <ul>
 *   <li>Before dag is populated</li>
 *   <li>After dag is populated</li>
 *   <li>Before dag is configured</li>
 *   <li>After dag is configured</li>
 *   <li>Before dag is validated</li>
 *   <li>After dag is validated</li>
 * </ul>
 *
 * @since 3.6.0
 */
@Evolving
public interface DAGSetupPlugin<T extends DAGSetupPlugin.Context> extends Plugin<T>
{
  /**
   * The context for the setup plugins
   */
  @Evolving
  interface Context<E extends DAGSetupEvent> extends PluginContext<DAGSetupEvent.Type, E>
  {

    DAG getDAG();

    Configuration getConfiguration();
  }
}

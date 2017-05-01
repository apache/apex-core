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

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;

/**
 * An Apex plugin is user code which runs inside the Apex engine. Plugin implementations implement this interface.
 *
 * Plugins can identify extension points by registering interest in events in the {@link Component#setup(Context)}
 * initialization method. They should also cleanup any additional resources created during shutdown such as helper
 * threads and open files in the {@link Component#teardown()} method.
 * @param <T> plugin context type
 *
 * @since 3.6.0
 */
@Evolving
public interface Plugin<T extends Plugin.PluginContext> extends Component<T>
{

  /**
   * An Apex plugin is user code which runs inside the Apex engine. The interaction between plugin and engine is managed
   * by PluginContext. Plugins can register interest in different events in the engine using the
   * ${@link PluginContext#register(Event.Type, EventHandler)} method.
   *
   * @param <T> the type of the Event.Type
   * @param <E> the event type
   */
  @Evolving
  interface PluginContext<T extends Event.Type, E extends Event<T>> extends Context
  {

    /**
     * Register interest in an event.
     *
     * Plugins register interest in events using this method. They would need to specify the event type and a handler to
     * handle the event, that would get called when the event occurs. A plugin can register interest in several events but
     * should register only a single handler for any specific event. In case register is called multiple times with the
     * same event type, then the last registered handler will be used.
     *
     * When an event occurs the
     * {@link EventHandler#handle(Event event)} method gets called with the event data.
     *
     * @param type The event type
     * @param handler The event handler
     */
    void register(T type, EventHandler<E> handler);
  }

  /**
   * A handler that handles an event in the Apex engine. Plugins register interest in events by registering handlers
   * using the PluginContext.
   * @param <E> The event type
   */
  @Evolving
  interface EventHandler<E extends Event>
  {
    /**
     * Handle a event.
     *
     * This method is called when the event occurs.
     *
     * @param event
     */
    void handle(E event);
  }
}

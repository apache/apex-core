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
package org.apache.apex.engine.plugin;

import org.apache.apex.api.plugin.Event;
import org.apache.hadoop.service.Service;

import com.datatorrent.api.DAG;

/**
 * @since 3.6.0
 */
public interface ApexPluginDispatcher extends Service
{

  /**
   * This is internal event, which is not delivered to the plugins.
   */
  Event.Type DAG_CHANGE = new Event.Type(){};

  class DAGChangeEvent extends Event.BaseEvent<Event.Type>
  {
    final DAG dag;

    public DAGChangeEvent(DAG dag)
    {
      super(DAG_CHANGE);
      this.dag = dag;
    }
  }

  void dispatch(Event e);
}

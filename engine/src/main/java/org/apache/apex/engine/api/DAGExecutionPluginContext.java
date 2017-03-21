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
package org.apache.apex.engine.api;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener.BatchedOperatorStats;
import com.datatorrent.common.util.Pair;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;
import com.datatorrent.stram.webapp.AppInfo;
import com.datatorrent.stram.webapp.LogicalOperatorInfo;

/**
 * An Apex plugin is a user code which runs inside Stram. The interaction
 * between plugin and Stram is managed by DAGExecutionPluginContext. Plugin can register to handle event in interest
 * with callback handler using ${@link DAGExecutionPluginContext#register(DAGExecutionPluginContext.RegistrationType, DAGExecutionPluginContext.Handler)}
 *
 * Following events are supported
 * <ul>
 *   <li>{@see DAGExecutionPluginContext.HEARTBEAT} The heartbeat from a container is delivered to the plugin after it has been handled by stram</li>
 *   <li>{@see DAGExecutionPluginContext.STRAM_EVENT} All the Stram event generated in Stram will be delivered to the plugin</li>
 *   <li>{@see DAGExecutionPluginContext.COMMIT_EVENT} When committedWindowId changes in the platform an event will be delivered to the plugin</li>
 * </ul>
 *
 * A plugin should register a single handler for an event, In case multiple handlers are registered for an event,
 * then the last registered handler will be used. Plugin should cleanup additional resources created by it during shutdown
 * such as helper threads and open files.
 */
@InterfaceStability.Evolving
public interface DAGExecutionPluginContext extends Context
{
  class RegistrationType<T>
  {
  }

  RegistrationType<StreamingContainerUmbilicalProtocol.ContainerHeartbeat> HEARTBEAT = new RegistrationType<>();
  RegistrationType<StramEvent> STRAM_EVENT = new RegistrationType<>();
  RegistrationType<Long> COMMIT_EVENT = new RegistrationType<>();

  <T> void register(RegistrationType<T> type, Handler<T> handler);

  interface Handler<T>
  {
    void handle(T data);
  }

  public StramAppContext getApplicationContext();

  public AppInfo.AppStats getApplicationStats();

  public Configuration getLaunchConfig();

  public DAG getDAG();

  public String getOperatorName(int id);

  public BatchedOperatorStats getPhysicalOperatorStats(int id);

  public List<LogicalOperatorInfo> getLogicalOperatorInfoList();

  public Queue<Pair<Long, Map<String, Object>>> getWindowMetrics(String operatorName);

  public long windowIdToMillis(long windowId);
}

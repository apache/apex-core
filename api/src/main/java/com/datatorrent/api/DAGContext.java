/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import java.util.HashSet;
import java.util.Set;

/**
 * <p>DAGContext interface.</p>
 *
 * @since 0.3.2
 */
public interface DAGContext extends Context
{
  /**
   * Launch mode for the application.
   * Used in the client to set configuration depending on how the DAG is executed.
   */
  public static final String LAUNCH_MODE = "stram.launchmode";

  /** Constant <code>DEFAULT_HEARTBEAT_LISTENER_THREAD_COUNT=30</code> */
  public static final int DEFAULT_HEARTBEAT_LISTENER_THREAD_COUNT = 30;
  /** Constant <code>SUBDIR_CHECKPOINTS="checkpoints"</code> */
  public static final String SUBDIR_CHECKPOINTS = "checkpoints";
  /** Constant <code>SUBDIR_STATS="stats"</code> */
  public static final String SUBDIR_STATS = "stats";
  /** Constant <code>SUBDIR_EVENTS="events"</code> */
  public static final String SUBDIR_EVENTS = "events";
  /** Constant <code>DEFAULT_ALLOCATE_RESOURCE_TIMEOUT_MILLIS=60000</code> */
  public static final int DEFAULT_ALLOCATE_RESOURCE_TIMEOUT_MILLIS = 60000;

  /**
   * Name under which the application will be shown in the resource manager.
   * If not set, the default is the configuration Java class or property file name.
   */
  public static final AttributeKey<String> APPLICATION_NAME = new AttributeKey<String>("stram.appName", String.class);

  /**
   * Application instance identifier. An application with the same name can run in multiple instances, each with a unique identifier.
   * The identifier is set by the client that submits the application and can be used in operators along with the operator ID to segregate output etc.
   * When running in distributed mode, the value would be the Yarn application id as shown in the resource manager (example: <code>application_1355713111917_0002</code>).
   */
  public static final AttributeKey<String> APPLICATION_ID = new AttributeKey<String>("stram.appId", String.class);

  /**
   * Comma separated list of jar file dependencies to be deployed with the application.
   * The launcher will combine the list with built-in dependencies and those specified
   * via {@link com.datatorrent.api.annotation.ShipContainingJars} into the final set of resources
   * that are made available through the distributed file system to application master
   * and child containers.
   */
  public static final AttributeKey<String> LIBRARY_JARS = new AttributeKey<String>("stram.libjars", String.class);

  /**
   * The maximum number or containers (excluding the application master) that the application is allowed to request.
   * If the DAG plan requires less containers, remaining count won't be allocated from the resource manager.
   * Example: DAG with several operators and all inline streams would require one container,
   * only one container will be requested from the resource manager.
   */
  public static final AttributeKey<Integer> CONTAINERS_MAX_COUNT = new AttributeKey<Integer>("stram.maxContainers", Integer.class);

  /**
   * Dump extra debug information in launcher, master and containers.
   */
  public static final AttributeKey<Boolean> DEBUG = new AttributeKey<Boolean>("stram.debug", Boolean.class);

  /**
   * The amount of memory to be requested for streaming containers. Not used in local mode.
   */
  public static final AttributeKey<Integer> CONTAINER_MEMORY_MB = new AttributeKey<Integer>("stram.containerMemoryMB", Integer.class);
  /** Constant <code>CONTAINER_JVM_OPTIONS</code> */
  public static final AttributeKey<String> CONTAINER_JVM_OPTIONS = new AttributeKey<String>("stram.containerJvmOpts", String.class);

  /**
   * The amount of memory to be requested for the application master. Not used in local mode.
   */
  public static final AttributeKey<Integer> MASTER_MEMORY_MB = new AttributeKey<Integer>("stram.masterMemoryMB", Integer.class);

  /** Constant <code>STREAMING_WINDOW_SIZE_MILLIS</code> */
  public static final AttributeKey<Integer> STREAMING_WINDOW_SIZE_MILLIS = new AttributeKey<Integer>("stram.windowSizeMillis", Integer.class);
  /** Constant <code>CHECKPOINT_WINDOW_COUNT</code> */
  public static final AttributeKey<Integer> CHECKPOINT_WINDOW_COUNT = new AttributeKey<Integer>("stram.checkpointWindowCount", Integer.class);
  /** Constant <code>APPLICATION_PATH</code> */
  public static final AttributeKey<String> APPLICATION_PATH = new AttributeKey<String>("stram.appPath", String.class);
  /** Constant <code>TUPLE_RECORDING_PART_FILE_SIZE</code> */
  public static final AttributeKey<Integer> TUPLE_RECORDING_PART_FILE_SIZE = new AttributeKey<Integer>("stram.tupleRecordingPartFileSize", Integer.class);
  /** Constant <code>TUPLE_RECORDING_PART_FILE_TIME_MILLIS</code> */
  public static final AttributeKey<Integer> TUPLE_RECORDING_PART_FILE_TIME_MILLIS = new AttributeKey<Integer>("stram.tupleRecordingPartFileTimeMillis", Integer.class);
  /** Constant <code>DAEMON_ADDRESS</code> */
  public static final AttributeKey<String> DAEMON_ADDRESS = new AttributeKey<String>("stram.daemon.address", String.class);
  /** Constant <code>FAST_PUBLISHER_SUBSCRIBER</code> */
  public static final AttributeKey<Boolean> FAST_PUBLISHER_SUBSCRIBER = new AttributeKey<Boolean>("stram.bufferserver.fast", Boolean.class);
  /**
   * How frequently should operators heartbeat to stram. Recommended setting is
   * 1000ms. Value 0 will disable heartbeat (for unit testing).
   */
  public static final AttributeKey<Integer> HEARTBEAT_INTERVAL_MILLIS = new AttributeKey<Integer>("stram.heartbeatIntervalMillis", Integer.class);

  /**
   * Timeout for master to identify a hung container (full GC etc.). Timeout will result in container restart.
   */
  public static final AttributeKey<Integer> HEARTBEAT_TIMEOUT_MILLIS = new AttributeKey<Integer>("stram.heartbeatTimeoutMillis", Integer.class);

  /**
   * Timeout for allocating container resources.
   */
  public static final AttributeKey<Integer> RESOURCE_ALLOCATION_TIMEOUT_MILLIS = new AttributeKey<Integer>("stram.allocateResourceTimeoutMillis", Integer.class);

  /** Constant <code>STATS_MAX_ALLOWABLE_WINDOWS_LAG</code> */
  public static final AttributeKey<Integer> STATS_MAX_ALLOWABLE_WINDOWS_LAG = new AttributeKey<Integer>("stram.maxWindowsBehindForStats", Integer.class);

  /** Constant <code>STATS_RECORD_INTERVAL_MILLIS</code> */
  public static final AttributeKey<Integer> STATS_RECORD_INTERVAL_MILLIS = new AttributeKey<Integer>("stram.recordStatsIntervalMillis", Integer.class);

  /** Constant <code>ATTRIBUTE_KEYS</code> */
  public final static Set<AttributeKey<?>> ATTRIBUTE_KEYS = AttributeKey.INSTANCES;

  public class AttributeKey<T> extends AttributeMap.AttributeKey<T> {
    public final Class<T> attributeType;
    private final static Set<AttributeKey<?>> INSTANCES = new HashSet<AttributeKey<?>>();

    @SuppressWarnings("LeakingThisInConstructor")
    private AttributeKey(String name, Class<T> type) {
      super(DAGContext.class, name);
      this.attributeType = type;
      INSTANCES.add(this);
    }
  }

}

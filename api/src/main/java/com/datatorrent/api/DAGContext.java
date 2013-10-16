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

import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.AttributeMap.AttributeInitializer;
import com.datatorrent.api.StringCodec.String2String;

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
  String LAUNCH_MODE = "stram.launchmode";
  /**
   * Name under which the application will be shown in the resource manager.
   * If not set, the default is the configuration Java class or property file name.
   */
  Attribute<String> APPLICATION_NAME = new Attribute<String>(new String2String());
  /**
   * Application instance identifier. An application with the same name can run in multiple instances, each with a unique identifier.
   * The identifier is set by the client that submits the application and can be used in operators along with the operator ID to segregate output etc.
   * When running in distributed mode, the value would be the Yarn application id as shown in the resource manager (example:
   * <code>application_1355713111917_0002</code>).
   */
  Attribute<String> APPLICATION_ID = new Attribute<String>(new String2String());
  /**
   * Comma separated list of jar file dependencies to be deployed with the application.
   * The launcher will combine the list with built-in dependencies and those specified
   * via {@link com.datatorrent.api.annotation.ShipContainingJars} into the final set of resources
   * that are made available through the distributed file system to application master
   * and child containers.
   */
  Attribute<String> LIBRARY_JARS = new Attribute<String>(new String2String());
  /**
   * The maximum number or containers (excluding the application master) that the application is allowed to request.
   * If the DAG plan requires less containers, remaining count won't be allocated from the resource manager.
   * Example: DAG with several operators and all inline streams would require one container,
   * only one container will be requested from the resource manager.
   */
  Attribute<Integer> CONTAINERS_MAX_COUNT = new Attribute<Integer>(Integer.MAX_VALUE);
  /**
   * Dump extra debug information in launcher, master and containers.
   */
  Attribute<Boolean> DEBUG = new Attribute<Boolean>(false);
  /**
   * The amount of memory to be requested for streaming containers. Not used in local mode.
   */
  Attribute<Integer> CONTAINER_MEMORY_MB = new Attribute<Integer>(2048);
  /**
   * Constant
   * <code>CONTAINER_JVM_OPTIONS</code>
   */
  Attribute<String> CONTAINER_JVM_OPTIONS = new Attribute<String>(new String2String());
  /**
   * The amount of memory to be requested for the application master. Not used in local mode.
   */
  Attribute<Integer> MASTER_MEMORY_MB = new Attribute<Integer>(2048);
  /**
   * Constant
   * <code>STREAMING_WINDOW_SIZE_MILLIS</code>
   */
  Attribute<Integer> STREAMING_WINDOW_SIZE_MILLIS = new Attribute<Integer>(500);
  /**
   * Constant
   * <code>CHECKPOINT_WINDOW_COUNT</code>
   */
  Attribute<Integer> CHECKPOINT_WINDOW_COUNT = new Attribute<Integer>(60);
  /**
   * Constant
   * <code>APPLICATION_PATH</code>
   */
  Attribute<String> APPLICATION_PATH = new Attribute<String>("unknown");
  /**
   * Constant
   * <code>TUPLE_RECORDING_PART_FILE_SIZE</code>
   */
  Attribute<Integer> TUPLE_RECORDING_PART_FILE_SIZE = new Attribute<Integer>(128 * 1024);
  /**
   * Constant
   * <code>TUPLE_RECORDING_PART_FILE_TIME_MILLIS</code>
   */
  Attribute<Integer> TUPLE_RECORDING_PART_FILE_TIME_MILLIS = new Attribute<Integer>(30 * 60 * 60 * 1000);
  /**
   * Constant
   * <code>GATEWAY_ADDRESS</code>
   */
  Attribute<String> GATEWAY_ADDRESS = new Attribute<String>(new String2String());
  /**
   * Constant
   * <code>FAST_PUBLISHER_SUBSCRIBER</code>
   */
  Attribute<Boolean> FAST_PUBLISHER_SUBSCRIBER = new Attribute<Boolean>(false);
  /**
   * Maximum number of simultaneous heartbeat connections to process.
   */
  Attribute<Integer> HEARTBEAT_LISTENER_THREAD_COUNT = new Attribute<Integer>(30);
  /**
   * How frequently should operators heartbeat to stram. Recommended setting is
   * 1000ms. Value 0 will disable heartbeat (for unit testing).
   */
  Attribute<Integer> HEARTBEAT_INTERVAL_MILLIS = new Attribute<Integer>(1000);
  /**
   * Timeout for master to identify a hung container (full GC etc.). Timeout will result in container restart.
   */
  Attribute<Integer> HEARTBEAT_TIMEOUT_MILLIS = new Attribute<Integer>(30 * 1000);
  /**
   * Timeout for allocating container resources.
   */
  Attribute<Integer> RESOURCE_ALLOCATION_TIMEOUT_MILLIS = new Attribute<Integer>(60000);
  /**
   * Constant
   * <code>STATS_MAX_ALLOWABLE_WINDOWS_LAG</code>
   */
  Attribute<Integer> STATS_MAX_ALLOWABLE_WINDOWS_LAG = new Attribute<Integer>(1000);
  /**
   * Constant
   * <code>STATS_RECORD_INTERVAL_MILLIS</code>
   */
  Attribute<Integer> STATS_RECORD_INTERVAL_MILLIS = new Attribute<Integer>(0);

  /** Constant <code>THROUGHPUT_CALCULATION_INTERVAL</code> */
  Attribute<Integer> THROUGHPUT_CALCULATION_INTERVAL = new Attribute<Integer>(10000);

  /** Constant <code>THROUGHPUT_CALCULATION_MAX_SAMPLES</code> */
  Attribute<Integer> THROUGHPUT_CALCULATION_MAX_SAMPLES = new Attribute<Integer>(1000);

  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  long serialVersionUID = AttributeInitializer.initialize(DAGContext.class);
}

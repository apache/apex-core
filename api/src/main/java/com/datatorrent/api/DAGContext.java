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
import com.datatorrent.api.StringCodec.Class2String;
import com.datatorrent.api.StringCodec.Map2String;
import com.datatorrent.api.StringCodec.String2String;
import java.util.Map;

/**
 * <p>DAGContext interface.</p>
 *
 * @since 0.3.2
 */
public interface DAGContext extends Context
{
  String DT_PREFIX = "dt.";
  /**
   * Launch mode for the application.
   * Used in the client to set configuration depending on how the DAG is executed.
   */
  String LAUNCH_MODE = DT_PREFIX + "launchmode";
  /**
   * Name under which the application will be shown in the resource manager.
   * If not set, the default is the configuration Java class or property file name.
   */
  Attribute<String> APPLICATION_NAME = new Attribute<String>("unknown-application-name");
  /**
   * URL to the application's documentation.
   * If not set, "app-documentation-unavailable" is the default.
   */
  Attribute<String> APPLICATION_DOC_LINK = new Attribute<String>("app-documentation-unavailable");
  /**
   * Application instance identifier. An application with the same name can run in multiple instances, each with a
   * unique identifier. The identifier is set by the client that submits the application and can be used in operators
   * along with the operator ID to segregate output etc.
   * <p>
   * When running in distributed mode, the value is the YARN application id as shown in the resource manager (example:
   * <code>application_1355713111917_0002</code>). Note that only the full id string uniquely identifies an application,
   * the integer offset will reset on RM restart.
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
   * Comma separated list of files to be deployed with the application.
   * The launcher will include the files into the final set of resources
   * that are made available through the distributed file system to application master
   * and child containers.
   */
  Attribute<String> FILES = new Attribute<String>(new String2String());
  /**
   * Comma separated list of archives to be deployed with the application.
   * The launcher will include the archives into the final set of resources
   * that are made available through the distributed file system to application master
   * and child containers.
   */
  Attribute<String> ARCHIVES = new Attribute<String>(new String2String());
  /**
   * The maximum number of containers (excluding the application master) that the application is allowed to request.
   * If the DAG plan requires less containers, remaining count won't be allocated from the resource manager.
   * Example: DAG with several operators and all streams container local would require one container,
   * only one container will be requested from the resource manager.
   */
  Attribute<Integer> CONTAINERS_MAX_COUNT = new Attribute<Integer>(Integer.MAX_VALUE);
  /**
   * Dump extra debug information in launcher, master and containers.
   */
  Attribute<Boolean> DEBUG = new Attribute<Boolean>(false);
  /**
   * The amount of memory to be requested for streaming containers. Not used in local mode.
   * Default value is 1GB.
   */
  Attribute<Integer> CONTAINER_MEMORY_MB = new Attribute<Integer>(1024);
  /**
   * The options to be pass to JVM when launching the containers. Options such as java maximum heap size can be specified here.
   */
  Attribute<String> CONTAINER_JVM_OPTIONS = new Attribute<String>(new String2String());
  /**
   * The amount of memory to be requested for the application master. Not used in local mode.
   * Default value is 1GB.
   */
  Attribute<Integer> MASTER_MEMORY_MB = new Attribute<Integer>(1024);
  /**
   * The streaming window size to use for the application. It is specified in milliseconds. Default value is 500ms.
   */
  Attribute<Integer> STREAMING_WINDOW_SIZE_MILLIS = new Attribute<Integer>(500);
  /**
   * The time interval for saving the operator state. It is specified as a multiple of streaming windows. The operator
   * state is saved periodically with interval equal to the checkpoint interval. Default value is 60 streaming windows.
   */
  Attribute<Integer> CHECKPOINT_WINDOW_COUNT = new Attribute<Integer>(60);
  /**
   * The path to store application dependencies, recording and other generated files for application master and containers.
   */
  Attribute<String> APPLICATION_PATH = new Attribute<String>(new String2String());
  /**
   * The size limit for a file where tuple recordings are stored. When tuples are being recorded they are stored
   * in files. When a file size reaches this limit a new file is created and tuples start getting stored in the new file. Default value is 128k.
   */
  Attribute<Integer> TUPLE_RECORDING_PART_FILE_SIZE = new Attribute<Integer>(128 * 1024);
  /**
   * The time limit for a file where tuple recordings are stored. When tuples are being recorded they are stored
   * in files. When a tuple recording file creation time falls beyond the time limit window from the current time a new file
   * is created and the tuples start getting stored in the new file. Default value is 30hrs.
   */
  Attribute<Integer> TUPLE_RECORDING_PART_FILE_TIME_MILLIS = new Attribute<Integer>(30 * 60 * 60 * 1000);
  /**
   * Address to which the application side connects to DT Gateway, in the form of host:port. This will override "dt.gateway.address" in the configuration.
   */
  Attribute<String> GATEWAY_CONNECT_ADDRESS = new Attribute<String>(new String2String());
  /**
   * @deprecated Please use GATEWAY_CONNECT_ADDRESS
   */
  @Deprecated
  Attribute<String> GATEWAY_ADDRESS = new Attribute<String>(new String2String());
  /**
   * Maximum number of simultaneous heartbeat connections to process. Default value is 30.
   */
  Attribute<Integer> HEARTBEAT_LISTENER_THREAD_COUNT = new Attribute<Integer>(30);
  /**
   * How frequently should operators heartbeat to stram. Recommended setting is
   * 1000ms. Value 0 will disable heartbeat (for unit testing). Default value is 1000ms.
   */
  Attribute<Integer> HEARTBEAT_INTERVAL_MILLIS = new Attribute<Integer>(1000);
  /**
   * Timeout for master to identify a hung container (full GC etc.). Timeout will result in container restart.
   * Default value is 30s.
   */
  Attribute<Integer> HEARTBEAT_TIMEOUT_MILLIS = new Attribute<Integer>(30 * 1000);
  /**
   * Timeout for allocating container resources. Default value is 60s.
   */
  Attribute<Integer> RESOURCE_ALLOCATION_TIMEOUT_MILLIS = new Attribute<Integer>(Integer.MAX_VALUE);
  /**
   * Maximum number of windows that can be pending for statistics calculation. Statistics are computed when
   * the metrics are available from all operators for a window. If the information is not available from all operators then
   * the window is pending. When the number of pending windows reaches this limit the information for the oldest window
   * is purged. Default value is 1000 windows.
   */
  Attribute<Integer> STATS_MAX_ALLOWABLE_WINDOWS_LAG = new Attribute<Integer>(1000);
  /**
   * The time interval for recording statistics. The statistics are periodically recorded with interval equal to the stats
   * record interval. If the interval is specified as 0 then no statistics are recorded. The default value is 0.
   */
  Attribute<Integer> STATS_RECORD_INTERVAL_MILLIS = new Attribute<Integer>(0);
  /**
   * The time interval for throughput calculation. The throughput is periodically calculated with interval greater than or
   * equal to the throughput calculation interval. The default value is 10s.
   */
  Attribute<Integer> THROUGHPUT_CALCULATION_INTERVAL = new Attribute<Integer>(10000);
  /**
   * The maximum number of samples to use when calculating throughput. In practice fewer samples may be used
   * if the THROUGHPUT_CALCULATION_INTERVAL is exceeded. Default value is 1000 samples.
   */
  Attribute<Integer> THROUGHPUT_CALCULATION_MAX_SAMPLES = new Attribute<Integer>(1000);
  /**
   * The string codec map for classes that are to be set or get through properties as strings.
   * Only supports string codecs that have a constructor with no arguments
   */
  Attribute<Map<Class<?>, Class<? extends StringCodec<?>>>> STRING_CODECS = new Attribute<Map<Class<?>, Class<? extends StringCodec<?>>>>(new Map2String<Class<?>, Class<? extends StringCodec<?>>>(",", "=", new Class2String<Object>(), new Class2String<StringCodec<?>>()));
  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  long serialVersionUID = AttributeInitializer.initialize(DAGContext.class);
}

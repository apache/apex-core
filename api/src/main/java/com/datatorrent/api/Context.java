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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.Attribute.AttributeMap.AttributeInitializer;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.StringCodec.*;
import com.datatorrent.api.annotation.Stateless;

/**
 * The base interface for context for all of the streaming platform objects<p>
 * <br>
 *
 * @since 0.3.2
 */
public interface Context
{
  /**
   * Get the attributes associated with this context.
   * The returned map does not contain any attributes that may have been defined in the parent context of this context.
   *
   * @return attributes defined for the current context.
   */
  public com.datatorrent.api.Attribute.AttributeMap getAttributes();

  /**
   * Get the value of the attribute associated with the current key by recursively traversing the contexts upwards to
   * the application level. If the attribute is not found, then return the defaultValue.
   *
   * @param <T> - Type of the value stored against the attribute
   * @param key - Attribute to identify the attribute.
   * @return The value for the attribute if found or the defaultValue passed in as argument.
   */
  public <T> T getValue(Attribute<T> key);

  /**
   * Custom stats provided by the operator implementation. Reported as part of operator stats in the context of the
   * current window, reset at window boundary.
   *
   * @param counters
   */
  void setCounters(Object counters);

  interface CountersAggregator
  {
    Object aggregate(Collection<?> countersList);

  }

  /**
   * The interface to control the container JVM Opts based on the operator(s) configuration
   */
  public interface ContainerOptConfigurator extends Serializable
  {
    /**
     * Get the container JVM opts based on the operator(s) configuration.
     * @param operatorMetaList The list of operators that are assigned to the container
     * @return The JVM options for the container
     */
    String getJVMOptions(List<DAG.OperatorMeta> operatorMetaList);
  }

  public interface PortContext extends Context
  {
    /**
     * Number of tuples the poll buffer can cache without blocking the input stream to the port.
     */
    Attribute<Integer> QUEUE_CAPACITY = new Attribute<Integer>(1024);
    /**
     * Poll period in milliseconds when the port buffer reaches its limits.
     */
    Attribute<Integer> SPIN_MILLIS = new Attribute<Integer>(10);
    /**
     * Input port attribute. Extend partitioning of an upstream operator w/o intermediate merge.
     * Can be used to form parallel partitions that span a group of operators.
     * Defined on input port to allow for stream to be shared with non-partitioned sinks.
     * If multiple ports of an operator have the setting, incoming streams must track back to
     * a common root partition, i.e. the operator join forks of the same origin.
     */
    Attribute<Boolean> PARTITION_PARALLEL = new Attribute<Boolean>(false);
    /**
     * Attribute of output port to specify how many partitions should be merged by a single unifier instance. If the
     * number of partitions exceeds the limit set, a cascading unifier plan will be created. For example, 4 partitions
     * with the limit set to 2 will result in 3 unifiers arranged in 2 levels. The setting can be used to cap the
     * network I/O or other resource requirement for each unifier container (depends on the specific functionality of
     * the unifier), enabling horizontal scale by overcoming the single unifier bottleneck.
     */
    Attribute<Integer> UNIFIER_LIMIT = new Attribute<Integer>(Integer.MAX_VALUE);

    /**
     * Attribute to specify that the final unifier be always a single unifier. This is useful when in MxN partitioning
     * case there is a need to unify all the outputs of the M stage into a single unifier before sending the results to
     * the N stage. The attribute can be specified either on the output port or the input port, the output port being
     * the usual. The specification on the input port overrides that specified on the output port. This is useful in
     * cases when an output port is connected to multiple input ports and different unifier behavior is desired for
     * the inputs. In this case the default unifier behavior can be specified on the output port and individual
     * exceptions can be specified on the corresponding input ports.
     */
    Attribute<Boolean> UNIFIER_SINGLE_FINAL = new Attribute<Boolean>(Boolean.FALSE);
    /**
     * Whether or not to auto record the tuples
     */
    Attribute<Boolean> AUTO_RECORD = new Attribute<Boolean>(false);
    /**
     * Whether the output is unified.
     * This is a read-only attribute to query that whether the output of the operator from multiple instances is being unified.
     */
    Attribute<Boolean> IS_OUTPUT_UNIFIED = new Attribute<Boolean>(false);
    /**
     * Provide the codec which can be used to serialize or deserialize the data
     * that can be received on the port. If it is unspecified the engine may use
     * a generic codec.
     */
    Attribute<StreamCodec<?>> STREAM_CODEC = new Attribute<StreamCodec<?>>(new Object2String<StreamCodec<?>>());
    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    long serialVersionUID = AttributeInitializer.initialize(PortContext.class);
  }

  public interface OperatorContext extends Context
  {
    /**
     * The windowId at which the operator's current run got activated.
     * When the operator is deployed the first time during it's activation, this value is the default value
     * of the operator. On subsequent run, it's the windowId of the checkpoint from which the operator state
     * is recovered.
     */
    Attribute<Long> ACTIVATION_WINDOW_ID = new Attribute<Long>(Stateless.WINDOW_ID);
    /**
     * Poll period in milliseconds when there are no tuples available on any of the input ports of the operator.
     * Default value is 10 milliseconds.
     */
    Attribute<Integer> SPIN_MILLIS = new Attribute<Integer>(10);
    /**
     * The maximum number of attempts to restart a failing operator before shutting down the application.
     * Until this number is reached, when an operator fails to start it is re-spawned in a new container. Once all the
     * attempts are exhausted, the application is shutdown. The default value for this attribute is null or unset and
     * is equivalent to infinity; The operator hence will be attempted to be recovered indefinitely unless this value
     * is set to anything else.
     */
    Attribute<Integer> RECOVERY_ATTEMPTS = new Attribute<Integer>(new Integer2String());
    /**
     * Specify a listener to process and optionally react to operator status updates.
     * The handler will be called for each physical operator as statistics are updated during heartbeat processing.
     */
    Attribute<Collection<StatsListener>> STATS_LISTENERS = new Attribute<Collection<StatsListener>>(new Collection2String<StatsListener>(",", new Object2String<StatsListener>(":")));
    /**
     * Conveys whether the Operator is stateful or stateless. If the operator is stateless, no checkpointing is required
     * by the engine. The attribute is ignored when the operator was already declared stateless through the
     * {@link Stateless} annotation.
     */
    Attribute<Boolean> STATELESS = new Attribute<Boolean>(false);
    /**
     * Memory resource that the operator requires for optimal functioning. Used to calculate total memory requirement for containers.
     */
    Attribute<Integer> MEMORY_MB = new Attribute<Integer>(1024);
    /**
     * The options to be pass to JVM when launching the operator. Options such as java maximum heap size can be specified here.
     */
    Attribute<String> JVM_OPTIONS = new Attribute<String>(new String2String());

    /**
     * Attribute of the operator that tells the platform how many streaming windows make 1 application window.
     */
    Attribute<Integer> APPLICATION_WINDOW_COUNT = new Attribute<Integer>(1);
    /**
     * Attribute of the operator that hints at the optimal checkpoint boundary.
     * By default checkpointing happens after every predetermined streaming windows. Application developer can override
     * this behavior by defining the following attribute. When this attribute is defined, checkpointing will be done after
     * completion of later of regular checkpointing window and the window whose serial number is divisible by the attribute
     * value. Typically user would define this value to be the same as that of APPLICATION_WINDOW_COUNT so checkpointing
     * will be done at application window boundary.
     */
    Attribute<Integer> CHECKPOINT_WINDOW_COUNT = new Attribute<Integer>(1);
    /**
     * Name of host to directly control locality of an operator. Complementary to stream locality (NODE_LOCAL affinity).
     * For example, the user may wish to specify a locality constraint for an input operator relative to its data source.
     * The attribute can then be set to the host name that is specified in the operator specific connect string property.
     */
    Attribute<String> LOCALITY_HOST = new Attribute<String>(new String2String());
    /**
     * Name of rack to directly control locality of an operator. Complementary to stream locality (RACK_LOCAL affinity).
     */
    Attribute<String> LOCALITY_RACK = new Attribute<String>(new String2String());
    /**
     * The agent which can be used to checkpoint the windows.
     */
    Attribute<StorageAgent> STORAGE_AGENT = new Attribute<StorageAgent>(new Object2String<StorageAgent>());
    /**
     * The payload processing mode for this operator - at most once, exactly once, or default at least once.
     * If the processing mode for an operator is specified as AT_MOST_ONCE and no processing mode is specified for the downstream
     * operators if any, the processing mode of the downstream operators is automatically set to AT_MOST_ONCE. If a different processing
     * mode is specified for the downstream operators it will result in an error.
     * If the processing mode for an operator is specified as EXACTLY_ONCE then the processing mode for all downstream operators
     * should be specified as AT_MOST_ONCE otherwise it will result in an error.
     */
    Attribute<Operator.ProcessingMode> PROCESSING_MODE = new Attribute<Operator.ProcessingMode>(ProcessingMode.AT_LEAST_ONCE);
    /**
     * Timeout to identify stalled processing, specified as count of streaming windows. If the last processed
     * window does not advance within the specified timeout count, the operator will be considered stuck and the
     * container restart. There are multiple reasons this could happen: clock drift, hardware issue, networking issue,
     * blocking operator logic, etc.
     */
    Attribute<Integer> TIMEOUT_WINDOW_COUNT = new Attribute<Integer>(120);
    /**
     * Whether or not to auto record the tuples
     */
    Attribute<Boolean> AUTO_RECORD = new Attribute<Boolean>(false);
    /**
     * How the operator distributes its state and share the input can be influenced by setting the Partitioner attribute.
     * If this attribute is set to non null value, the instance of the partitioner is used to partition and merge the
     * state of the operator and the inputs. If this attribute is set to null then default partitioning is used.
     * If the attribute is not set and the operator implements Partitioner interface, then the instance of the operator
     * is used otherwise default default partitioning is used.
     */
    Attribute<Partitioner<? extends Operator>> PARTITIONER = new Attribute<Partitioner<? extends Operator>>(new Object2String<Partitioner<? extends Operator>>());

    /**
     * Aggregates physical counters to a logical counter.
     */
    Attribute<CountersAggregator> COUNTERS_AGGREGATOR = new Attribute<CountersAggregator>(new Object2String<CountersAggregator>());

    /**
     * Return the operator runtime id.
     *
     * @return The id
     */
    int getId();

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    long serialVersionUID = AttributeInitializer.initialize(OperatorContext.class);
  }

  /**
   * <p>
   * DAGContext interface.</p>
   *
   * @since 0.3.2
   */
  interface DAGContext extends Context
  {
    /**
     * Name under which the application will be shown in the resource manager.
     * If not set, the default is the configuration Java class or property file name.
     */
    Attribute<String> APPLICATION_NAME = new Attribute<String>("unknown-application-name");
    /**
     * URL to the application's documentation.
     */
    Attribute<String> APPLICATION_DOC_LINK = new Attribute<String>(new String2String());

    /**
     * URL to the application's app data, if any. If not set, an empty string is the default.
     * <p>
     * Please note that if the string <code>"{appId}"</code> is present in this atttribute value, the
     * DataTorrent UI Console will replace it with the full application ID. For example, if it is set
     * to <code>"http://mynetwork.net/my/appdata/dashboard?appId={appId}"</code>, it will be converted to
     * <code>"http://mynetwork.net/my/appdata/dashboard?appId=application_1355713111917_0002"</code>.
     * </p>
     */
    Attribute<String> APPLICATION_DATA_LINK = new Attribute<String>(new String2String());
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
     * The options to be pass to JVM when launching the containers. Options such as java maximum heap size can be specified here.
     */
    Attribute<String> CONTAINER_JVM_OPTIONS = new Attribute<String>(new String2String());
    /**
     * The amount of memory to be requested for the application master. Not used in local mode.
     * Default value is 1GB.
     */
    Attribute<Integer> MASTER_MEMORY_MB = new Attribute<Integer>(1024);
    /**
     * The amount of memory each buffer server will try to use at maximum. There is a buffer server in each container,
     * so the memory allocated here directly affects the RAM available for the rest of the operators running in the container.
     * Also due to the nature of the application, if buffer server needs to use more RAM, from time to time, this number may
     * not be adhered to.
     */
    Attribute<Integer> BUFFER_SERVER_MEMORY_MB = new Attribute<Integer>(8 * 64);
    /**
     * Where to spool the data once the buffer server capacity is reached.
     */
    Attribute<Boolean> EXPERIMENTAL_BUFFER_SPOOLING = new Attribute<Boolean>(true);
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
     * Address to which the application side connects to DT Gateway, in the form of host:port. This will override "dt.gateway.listenAddress" in the configuration.
     */
    Attribute<String> GATEWAY_CONNECT_ADDRESS = new Attribute<String>(new String2String());
    /**
     * Whether or not gateway is expecting SSL connection.
     */
    Attribute<Boolean> GATEWAY_USE_SSL = new Attribute<Boolean>(false);
    /**
     * The username for logging in to the gateway, if authentication is enabled.
     */
    Attribute<String> GATEWAY_USER_NAME = new Attribute<String>(new String2String());
    /**
     * The password for logging in to the gateway, if authentication is enabled.
     */
    Attribute<String> GATEWAY_PASSWORD = new Attribute<String>(new String2String());
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
     * Whether or not we record statistics. The statistics are recorded for each heartbeat if enabled. The default value is false.
     */
    Attribute<Boolean> ENABLE_STATS_RECORDING = new Attribute<Boolean>(false);
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
     * The agent which can be used to find the jvm options for the container.
     */
    Attribute<ContainerOptConfigurator> CONTAINER_OPTS_CONFIGURATOR = new Attribute<ContainerOptConfigurator>(new Object2String<ContainerOptConfigurator>());
    /**
     * The string codec map for classes that are to be set or get through properties as strings.
     * Only supports string codecs that have a constructor with no arguments
     */
    Attribute<Map<Class<?>, Class<? extends StringCodec<?>>>> STRING_CODECS = new Attribute<Map<Class<?>, Class<? extends StringCodec<?>>>>(new Map2String<Class<?>, Class<? extends StringCodec<?>>>(",", "=", new Class2String<Object>(), new Class2String<StringCodec<?>>()));
    @SuppressWarnings(value = "FieldNameHidesFieldInSuperclass")
    long serialVersionUID = AttributeInitializer.initialize(DAGContext.class);
  }

  long serialVersionUID = AttributeInitializer.initialize(Context.class);
}

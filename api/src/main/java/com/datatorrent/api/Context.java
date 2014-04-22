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

import java.util.Collection;

import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.AttributeMap.AttributeInitializer;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.StringCodec.Collection2String;
import com.datatorrent.api.StringCodec.Object2String;
import com.datatorrent.api.StringCodec.String2String;
import com.datatorrent.api.annotation.Stateless;

/**
 *
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
  public AttributeMap getAttributes();

  /**
   * Get the value of the attribute associated with the current key by recursively traversing the contexts upwards to
   * the application level. If the attribute is not found, then return the defaultValue.
   *
   * @param <T> - Type of the value stored against the attribute
   * @param key - Attribute to identify the attribute.
   * @return The value for the attribute if found or the defaultValue passed in as argument.
   */
  public <T> T getValue(AttributeMap.Attribute<T> key);

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
     * When an operator fails to start it is re-spawned in a new container. If it continues to fail after the number of restart
     * attempts reaches this limit the application is shutdown. The default value is 5 attempts.
     */
    Attribute<Integer> RECOVERY_ATTEMPTS = new Attribute<Integer>(5);
    /**
     * Count of initial partitions for the operator. The number is interpreted as follows:
     * <p>
     * Default partitioning (operator does not implement {@link Partitionable}):<br>
     * The platform creates the initial partitions by cloning the operator from the logical plan.<br>
     * Default partitioning does not consider operator state on split or merge.
     * <p>
     * Operator implements {@link Partitionable}:<br>
     * Value given as initial capacity hint to {@link Partitionable#definePartitions(java.util.Collection, int)}
     * The operator implementation controls instance number and initialization on a per partition basis.
     */
    Attribute<Integer> INITIAL_PARTITION_COUNT = new Attribute<Integer>(1);
    /**
     * The minimum rate of tuples below which the physical operators are consolidated in dynamic partitioning. When this
     * attribute is set and partitioning is enabled if the number of tuples per second falls below the specified rate
     * the physical operators are consolidated into fewer operators till the rate goes above the specified minimum.
     */
    Attribute<Integer> PARTITION_TPS_MIN = new Attribute<Integer>(0);
    /**
     * The maximum rate of tuples above which new physical operators are spawned in dynamic partitioning. When this
     * attribute is set and partitioning is enabled if the number of tuples per second goes above the specified rate new
     * physical operators are spawned till the rate again goes below the specified maximum.
     */
    Attribute<Integer> PARTITION_TPS_MAX = new Attribute<Integer>(0);
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
     * Attribute of the operator that suggests the ideal RAM that the operator may need for optimal functioning.
     */
    //public static final Attribute<Integer> MEMORY_MB = new Attribute<Integer>("memoryMB");
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
     * Return the operator runtime id.
     *
     * @return The id
     */
    int getId();

    /**
     * Custom stats provided by the operator implementation. Reported as part of operator stats in the context of the
     * current window, reset at window boundary.
     *
     * @param stats
     */
    void setCustomStats(Stats.OperatorStats.CustomStats stats);

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    long serialVersionUID = AttributeInitializer.initialize(OperatorContext.class);
  }

  long serialVersionUID = AttributeInitializer.initialize(Context.class);
}

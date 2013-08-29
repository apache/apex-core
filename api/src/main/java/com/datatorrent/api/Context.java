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
   * @param <T> - Type of the attribute.
   * @param key - AttributeKey to identify the attribute.
   * @param defaultValue - Default value if the attribute is not found.
   * @return The value for the attribute if found or the defaultValue passed in as argument.
   */
  public <T> T attrValue(AttributeMap.AttributeKey<T> key, T defaultValue);

  public interface PortContext extends Context
  {
    public class AttributeKey<T> extends AttributeMap.AttributeKey<T>
    {
      private AttributeKey(String name)
      {
        super(PortContext.class, name);
      }

    }

    /**
     * Number of tuples the poll buffer can cache without blocking the input stream to the port.
     */
    public static final AttributeKey<Integer> QUEUE_CAPACITY = new AttributeKey<Integer>("queueCapacity");
    /**
     * Poll period in milliseconds when the port buffer reaches its limits.
     */
    public static final AttributeKey<Integer> SPIN_MILLIS = new AttributeKey<Integer>("spinMillis");
    /**
     * Extend partitioning of an upstream operator to this port w/o intermediate merge.
     * Can be used to form parallel partitions that span a groups of operators.
     * Defined on a per input port basis to allow for stream to be shared with non-partitioned sinks.
     * If multiple ports of an operator have the setting, incoming streams must track back to
     * a common root partition, i.e. the operator join forks of the same origin.
     * At the moment each partition would be deployed to a single container (inline).
     */
    public static final AttributeKey<Boolean> PARTITION_PARALLEL = new AttributeKey<Boolean>("partitionInline");
    /**
     * Whether or not to auto record the tuples
     */
    public static final AttributeKey<Boolean> AUTO_RECORD = new AttributeKey<Boolean>("autoRecord");
  }

  public interface OperatorContext extends Context
  {
    public class AttributeKey<T> extends AttributeMap.AttributeKey<T>
    {
      private AttributeKey(String name)
      {
        super(OperatorContext.class, name);
      }

    }

    public static final AttributeKey<Integer> SPIN_MILLIS = new AttributeKey<Integer>("spinMillis");
    public static final AttributeKey<Integer> RECOVERY_ATTEMPTS = new AttributeKey<Integer>("recoveryAttempts");
    /**
     * Initial partition count for an operator that supports partitioning. The
     * number is interpreted as follows:
     * <p>
     * Default partitioning (operators that do not implement
     * {@link PartitionableOperator}):<br>
     * If the attribute is not present or set to 0 partitioning is off. Else the
     * number of initial partitions (statically created during initialization).
     * <p>
     * Operator that implements {@link PartitionableOperator}:<br>
     * Count 0 disables partitioning. Other values are ignored as number of
     * initial partitions is determined by operator implementation.
     */
    public static final AttributeKey<Integer> INITIAL_PARTITION_COUNT = new AttributeKey<Integer>("initialPartitionCount");
    public static final AttributeKey<Integer> PARTITION_TPS_MIN = new AttributeKey<Integer>("partitionTpsMin");
    public static final AttributeKey<Integer> PARTITION_TPS_MAX = new AttributeKey<Integer>("partitionTpsMax");
    public static final AttributeKey<String> PARTITION_STATS_HANDLER = new AttributeKey<String>("statsHandler");
    /**
     * Attribute of the operator that conveys to the stram whether the Operator is stateful or stateless.
     */
    public static final AttributeKey<Boolean> STATELESS = new AttributeKey<Boolean>("stateless");
    /**
     * Attribute of the operator that suggests the ideal RAM that the operator may need for optimal functioning.
     */
    public static final AttributeKey<Integer> MEMORY_MB = new AttributeKey<Integer>("memoryMB");
    /**
     * Attribute of the operator that tells the platform how many streaming windows make 1 application window.
     */
    public static final AttributeKey<Integer> APPLICATION_WINDOW_COUNT = new AttributeKey<Integer>("applicationWindowCount");
    /**
     * Attribute of the operator that hints at the optimal checkpoint boundary.
     * By default checkpointing happens after every predetermined streaming windows. But application developer can override
     * this behavior by defining the following attribute. When this attribute is defined, checkpointing will be done after
     * completion of later of regular checkpointing window and the window whose serial number is divisible by the attribute
     * value. Typically user would define this value to be the same as that of APPLICATION_WINDOW_COUNT so checkpointing
     * will be done at application window boundary.
     */
    public static final AttributeKey<Integer> CHECKPOINT_WINDOW_COUNT = new AttributeKey<Integer>("checkpointWindowCount");
    /**
     * Logical name of a host to control locality between operators (even when not connected through stream)
     */
    public static final AttributeKey<String> LOCALITY_HOST = new AttributeKey<String>("localityHost");
    /**
     * Logical name of a rack to control locality between operators (even when not connected through stream)
     */
    public static final AttributeKey<String> LOCALITY_RACK = new AttributeKey<String>("localityRack");
    /**
     * The agent which can be used to checkpoint the windows.
     */
    public static final AttributeKey<StorageAgent> STORAGE_AGENT = new AttributeKey<StorageAgent>("backupAgent");
    /**
     * The payload processing mode for this operator - at most once, exactly once, or default at least once.
     */
    public static final AttributeKey<Operator.ProcessingMode> PROCESSING_MODE = new AttributeKey<Operator.ProcessingMode>("processMode");

    /**
     * Return the operator runtime id.
     *
     * @return String
     */
    int getId();

  }

}

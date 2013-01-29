/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import com.malhartech.util.AttributeMap;

/**
 *
 * The base interface for context for all of the streaming platform objects<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Context
{
  public interface PortContext extends Context
  {
    public class AttributeKey<T> extends AttributeMap.AttributeKey<PortContext, T>
    {
      private AttributeKey(String name)
      {
        super(PortContext.class, name);
      }

    }

    /**
     * Number of tuples the poll buffer can cache without blocking the input stream to the port.
     */
    public static final AttributeKey<Integer> BUFFER_SIZE = new AttributeKey<Integer>("bufferSize");
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

    AttributeMap<PortContext> getAttributes();

  }

  public interface OperatorContext extends Context
  {
    public class AttributeKey<T> extends AttributeMap.AttributeKey<OperatorContext, T>
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
     * number of initial partitions (statically created during initialization.
     * <p>
     * Operator that implements {@link PartitionableOperator}:<br>
     * Count 0 disables partitioning. Other values are ignored as number of
     * initial partitions is determined by operator implementation.
     */
    public static final AttributeKey<Integer> INITIAL_PARTITION_COUNT = new AttributeKey<Integer>("initialPartitionCount");
    public static final AttributeKey<Integer> PARTITION_TPS_MIN = new AttributeKey<Integer>("partitionTpsMin");
    public static final AttributeKey<Integer> PARTITION_TPS_MAX = new AttributeKey<Integer>("partitionTpsMax");
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
     * Return the operator runtime id.
     *
     * @return String
     */
    int getId();

    AttributeMap<OperatorContext> getAttributes();

    /**
     * Return the application level attributes.
     * This will be the same set for all operators in the system.
     *
     * @return
     */
    AttributeMap<DAGConstants> getApplicationAttributes();

  }

}

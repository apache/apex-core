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
  public interface PortContext extends Context {
    public static final AttributeKey<Integer> BUFFER_SIZE = new AttributeKey<Integer>("BUFFER_SIZE");

    public class AttributeKey<T> extends AttributeMap.AttributeKey<PortContext, T> {
      private AttributeKey(String name) {
        super(PortContext.class, name);
      }
    }

    AttributeMap<PortContext> getAttributes();

  }

  public interface OperatorContext extends Context {
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
     * initial partitions are determined by operator implementation.
     */
    public static final AttributeKey<Integer> INITIAL_PARTITION_COUNT = new AttributeKey<Integer>("initialPartitionCount");
    public static final AttributeKey<Integer> PARTITION_TPS_MIN = new AttributeKey<Integer>("partitionTpsMin");
    public static final AttributeKey<Integer> PARTITION_TPS_MAX = new AttributeKey<Integer>("partitionTpsMax");

    public class AttributeKey<T> extends AttributeMap.AttributeKey<OperatorContext, T> {
      private AttributeKey(String name) {
        super(OperatorContext.class, name);
      }
    }

    /**
     * Return the operator runtime id.
     * @return String
     */
    String getId();

    AttributeMap<OperatorContext> getAttributes();

  }

}

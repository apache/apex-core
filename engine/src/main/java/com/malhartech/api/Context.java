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

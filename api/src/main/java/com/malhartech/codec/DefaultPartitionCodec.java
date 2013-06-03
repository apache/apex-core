/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.codec;

import com.malhartech.common.KeyValPair;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class DefaultPartitionCodec<K, V> extends JavaSerializationStreamCodec<KeyValPair<K, V>>
{
  /**
   * A codec to enable partitioning to be done by key
   */
  @Override
  public int getPartition(KeyValPair<K, V> o)
  {
    return o.getKey().hashCode();
  }
}

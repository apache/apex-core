/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V>
{
  private static final long serialVersionUID = 1L;
  private int capacity; // Maximum number of items in the cache.

  public LRUCache(int capacity)
  {
    super(capacity + 1, 1.0f, true); // Pass 'true' for accessOrder.
    this.capacity = capacity;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> entry)
  {
    return (size() > this.capacity);
  }

}
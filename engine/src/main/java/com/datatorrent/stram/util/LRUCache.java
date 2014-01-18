/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>LRUCache class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.5
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V>
{
  private static final long serialVersionUID = 1L;
  private int capacity; // Maximum number of items in the cache.

  public LRUCache(int capacity, boolean accessOrder)
  {
    super(capacity + 1, 1.0f, accessOrder); // Pass 'true' for accessOrder.
    this.capacity = capacity;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> entry)
  {
    return (size() > this.capacity);
  }

}

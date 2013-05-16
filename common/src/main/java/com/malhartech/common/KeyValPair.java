/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.AbstractMap;

/**
 *
 * A single KeyValPair for basic data passing, It is a write once, and read often model. <p>
 * <br>
 * Key and Value are to be treated as immutable objects.
 *
 * @param <K>
 * @param <V>
 * @author amol<br>
 *
 */
public class KeyValPair<K, V> extends AbstractMap.SimpleEntry<K, V>
{
  private static final long serialVersionUID = 201301281547L;

  /**
   * Added default constructor for deserializer.
   */
  private KeyValPair()
  {
    super(null, null);
  }

  /**
   * Constructor
   *
   * @param k sets key
   * @param v sets value
   */
  public KeyValPair(K k, V v)
  {
    super(k, v);
  }

}

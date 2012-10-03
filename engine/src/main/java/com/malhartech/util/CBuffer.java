/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

/**
 * Temporary Interface to facilitate the testing. The interface the CircularBuffer should implement is
 * probably Queue or some other standard container interface.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface CBuffer<T>
{

  /**
   *
   * Add object at the head<p>
   * <br>
   *
   * @param toAdd object to be added
   *
   */
  void add(T toAdd);

  /**
   *
   * Get object from the tail<p>
   * <br>
   *
   * @return object removed from the buffer returned
   * <br>
   */
  T get();

  /**
   *
   * Number of objects in the buffer<p>
   * <br>
   *
   * @return Number of objects in the buffer
   * <br>
   */
  int size();

}

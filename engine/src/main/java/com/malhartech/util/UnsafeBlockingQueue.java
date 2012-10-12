/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

import java.util.concurrent.BlockingQueue;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface UnsafeBlockingQueue<T> extends BlockingQueue<T>
{
  /**
   * Retrieves and removes the head of this queue.
   *
   * This method should be called only when the callee knows that the head is present. It skips
   * the checks that poll does hence you may get unreliable results if you use it without checking
   * for the presence of the head first.
   *
   * @return the head of this queue.
   */
  public T pollUnsafe();
}

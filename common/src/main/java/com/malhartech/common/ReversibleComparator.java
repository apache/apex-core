/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.common;

import java.util.Comparator;

/**
 *
 * A comparator for ascending and descending lists<p>
 * <br>
 *
 * @author amol<br>
 *
 */

public class ReversibleComparator<E> implements Comparator<E>
{
  /**
   * Added default constructor for deserializer
   */
  public ReversibleComparator()
  {
  }

  /**
   *
   * @param flag true for ascending, false for descending
   */
  public ReversibleComparator(boolean flag)
  {
    ascending = flag;
  }
  public boolean ascending = true;

  /**
   * Compare function
   * @param e1
   * @param e2
   * @return e1.compareTo(e2) if acscending, else 0 - e1.compareTo(e2)
   */
  @Override
  public int compare(E e1, E e2)
  {
    Comparable<? super E> ce1 = (Comparable<? super E>)e1;
    int ret = ce1.compareTo(e2);
    if (!ascending) {
      ret = 0 - ret;
    }
    return ret;
  }
}

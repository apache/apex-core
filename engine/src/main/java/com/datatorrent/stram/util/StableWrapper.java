/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.util.Comparator;

/**
 * Used to wrap around long int values safely<p>
 * <br>
 * Needed to ensure that windowId wrap around safely<br>
 * {@see StablePriorityQueue}<br>
 * <br> 
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2 
 */


class StableWrapper<E>
{
  private static final long serialVersionUID = 201207091936L;
  public final int id;
  public final E object;

  public StableWrapper(E o, int id)
  {
    this.id = id;
    this.object = o;
  }
}

class StableWrapperNaturalComparator<E> implements Comparator<StableWrapper<E>>
{
  @Override
  public int compare(StableWrapper<E> o1, StableWrapper<E> o2)
  {
    @SuppressWarnings("unchecked")
    int ret = ((Comparable) o1.object).compareTo(o2.object);

    if (ret == 0) {
      if (o1.id > o2.id) {
        ret = 1;
      }
      else if (o1.id < o2.id) {
        ret = -1;
      }
    }

    return ret;
  }
}

class StableWrapperProvidedComparator<E> implements Comparator<StableWrapper<E>>
{
  public final Comparator<? super E> comparator;

  public StableWrapperProvidedComparator(Comparator<? super E> comparator)
  {
    this.comparator = comparator;
  }

  @Override
  public int compare(StableWrapper<E> o1, StableWrapper<E> o2)
  {
    int ret = comparator.compare(o1.object, o2.object);

    if (ret == 0) {
      if (o1.id > o2.id) {
        ret = 1;
      }
      else if (o1.id < o2.id) {
        ret = -1;
      }
    }

    return ret;
  }
}

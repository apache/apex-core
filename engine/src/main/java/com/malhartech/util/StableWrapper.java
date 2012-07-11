/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

import java.util.Comparator;


/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
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

/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.physical;

import java.io.Serializable;
import java.util.Arrays;
import java.util.IdentityHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MVCC store for single writer and multiple readers of consistent revisions.
 * All values are managed in arrays and referenced through an index map.
 * Fields can be added, but not removed.
 *
 * @since 0.9.3
 */
public class StatsRevisions implements Serializable
{
  private static final long serialVersionUID = 201401131642L;
  private static final Logger LOG = LoggerFactory.getLogger(StatsRevisions.class);

  private final IdentityHashMap<Object, Integer> longsIndex = new IdentityHashMap<Object, Integer>();
  private transient ThreadLocal<Revision> VERSION = new ThreadLocal<Revision>();
  private Revision current = new Revision();

  private class Revision implements Serializable
  {
    private static final long serialVersionUID = 201401131642L;
    Object[] refs;
    long[] longs;
    double[] doubles;

    private Revision()
    {
      refs = new Object[0];
      longs = new long[0];
      doubles = new double[0];
    }

    private Revision(Revision other)
    {
      refs = Arrays.copyOf(other.refs, other.refs.length);
      longs = Arrays.copyOf(other.longs, other.longs.length);
      doubles = Arrays.copyOf(other.doubles, other.doubles.length);
    }

  }

  public class VersionedLong implements Serializable
  {
    private static final long serialVersionUID = 201401131642L;
    private final int index;

    public VersionedLong()
    {
      this.index = longsIndex.size();
      longsIndex.put(this, index);
    }

    public long get()
    {
      Revision v = VERSION.get();
      if (v == null) {
        v = current;
      }
      if (index < v.longs.length) {
        return v.longs[index];
      }
      // revision did not have key
      return 0;
    }

    public void set(long val)
    {
      Revision v = VERSION.get();
      if (v == null || v == current) {
        throw new AssertionError("Cannot modify readonly state.");
      }
      if (index >= v.longs.length) {
        // grow array
        long[] newArray = new long[index + 10];
        System.arraycopy(v.longs, 0, newArray, 0, v.longs.length);
        v.longs = newArray;
        LOG.debug("new array length: " + v.longs.length);
      }
      v.longs[index] = val;
    }

    public void add(long val)
    {
      set(get()+val);
    }

    @Override
    public String toString()
    {
      return Long.toString(get());
    }

  }

  public void checkout() {
    Revision v = new Revision(current);
    VERSION.set(v);
  }

  public void commit() {
    //LOG.debug("commit " + this);
    Revision v = VERSION.get();
    current = v;
    VERSION.remove();
  }

}

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

/**
 *
 * A single KeyValPair for basic data passing, It is a write once, and read often model. <p>
 * <br>
 * Key and Value are to be treated as immutable objects.
 *
 * @author amol<br>
 *
 */
public class HighLow
{
  Number high;
  Number low;

  /**
   * Added default constructor for deserializer.
   */
  public HighLow()
  {
    high = null;
    low = null;
  }

  /**
   * Constructor
   *
   * @param h
   * @param l
   */
  public HighLow(Number h, Number l)
  {
    high = h;
    low = l;
  }

  /**
   * @return high value
   */
  public Number getHigh()
  {
    return high;
  }

  /**
   *
   * @return low value
   */
  public Number getLow()
  {
    return low;
  }

  /**
   * @param h sets high value
   */
  public void setHigh(Number h)
  {
    high = h;
  }

  /**
   *
   * @param l sets low value
   */
  public void setLow(Number l)
  {
    low = l;
  }

  @Override
  public String toString()
  {
    return "(" + low.toString() + "," + high.toString() + ")";
  }

}

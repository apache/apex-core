/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.common;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class CandleStick extends HighLow
{
  private Number open;
  private Number close;

  /**
   * Added default constructor for deserializer.
   */
  public CandleStick()
  {
    super();
    open = close = null;
  }

  /**
   * Constructor
   *
   * @param o
   * @param c
   * @param h
   * @param l
   */
  public CandleStick(Number o, Number c, Number h, Number l)
  {
    super(h,l);
    open = o;
    close = c;
  }

  public void reset(Number n)
  {
    open = close = high = low = n;
  }

  public Number getOpen()
  {
    return open;
  }

  public Number getClose()
  {
    return close;
  }

  public void setOpen(Number o)
  {
    open = o;
  }

  public void setClose(Number c)
  {
    close = c;
  }

}

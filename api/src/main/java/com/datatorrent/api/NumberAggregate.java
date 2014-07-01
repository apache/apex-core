/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.api;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public abstract class NumberAggregate
{
  protected int count = 0;

  public abstract Number getMin();
  public abstract Number getMax();
  public abstract Number getSum();
  public abstract Number getAvg();

  public static class LongAggregate extends NumberAggregate {
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private long sum = 0;
    private boolean ignoreSum = false;
    private boolean ignoreAvg = false;

    public LongAggregate()
    {
    }

    public LongAggregate(boolean ignoreSum, boolean ignoreAvg)
    {
      this.ignoreSum = ignoreSum;
      this.ignoreAvg = ignoreAvg;
    }

    public void addNumber(long num)
    {
      if (min > num) {
        min = num;
      }
      if (max < num) {
        max = num;
      }
      sum += num;
      count++;
    }

    @Override
    public Number getMin()
    {
      return (count == 0) ? null : min;
    }

    @Override
    public Number getMax()
    {
      return max;
    }

    @Override
    public Number getAvg()
    {
      return (count == 0 || ignoreAvg) ? null : (sum / count);
    }

    @Override
    public Number getSum()
    {
      return ignoreSum ? null : sum;
    }
  }


  public static class DoubleAggregate extends NumberAggregate {
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private double sum = 0;
    private boolean ignoreSum = false;
    private boolean ignoreAvg = false;

    public DoubleAggregate()
    {
    }

    public DoubleAggregate(boolean ignoreSum, boolean ignoreAvg)
    {
      this.ignoreSum = ignoreSum;
      this.ignoreAvg = ignoreAvg;
    }

    public void addNumber(double num)
    {
      if (min > num) {
        min = num;
      }
      if (max < num) {
        max = num;
      }
      sum += num;
      count++;
    }

    @Override
    public Number getMin()
    {
      return (count == 0) ? null : min;
    }

    @Override
    public Number getMax()
    {
      return max;
    }

    @Override
    public Number getAvg()
    {
      return (count == 0 || ignoreAvg) ? null : (sum / count);
    }

    @Override
    public Number getSum()
    {
      return ignoreSum ? null : sum;
    }
  }

}

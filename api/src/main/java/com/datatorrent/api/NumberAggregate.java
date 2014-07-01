/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api;

/**
 * Utility class that gives aggregate information (i.e. min, max, avg, sum) with a set of numbers.
 *
 * @since 1.0.2
 */
public abstract class NumberAggregate
{
  /**
   * How many numbers are there at this point.
   */
  protected int count = 0;

  /**
   * Gets the minimum of the given numbers.
   *
   * @return The min
   */
  public abstract Number getMin();

  /**
   * Gets the maximum of the given numbers
   *
   * @return The max
   */
  public abstract Number getMax();

  /**
   * Gets the sum of the given numbers
   *
   * @return The sum
   */
  public abstract Number getSum();

  /**
   * Gets the average of the given numbers
   *
   * @return The avg
   */
  public abstract Number getAvg();

  /**
   * This is the aggregate class for Long.
   */
  public static class LongAggregate extends NumberAggregate
  {
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private long sum = 0;
    private boolean ignoreSum = false;
    private boolean ignoreAvg = false;

    /**
     * Creates a LongAggregate, with sum and average both valid.
     */
    public LongAggregate()
    {
    }

    /**
     * Creates a LongAggregate
     *
     * @param ignoreSum whether or not sum is valid for the numbers.
     * @param ignoreAvg whether or not average is valid for the numbers.
     */
    public LongAggregate(boolean ignoreSum, boolean ignoreAvg)
    {
      this.ignoreSum = ignoreSum;
      this.ignoreAvg = ignoreAvg;
    }

    /**
     * Add a long to the number set
     *
     * @param num the number
     */
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

  /**
   * This is the aggregate class for Double.
   */
  public static class DoubleAggregate extends NumberAggregate
  {
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private double sum = 0;
    private boolean ignoreSum = false;
    private boolean ignoreAvg = false;

    /**
     * Creates a DoubleAggregate, with sum and average both valid.
     */
    public DoubleAggregate()
    {
    }

    /**
     * Creates a DoubleAggregate
     *
     * @param ignoreSum whether or not sum is valid for the numbers.
     * @param ignoreAvg whether or not average is valid for the numbers.
     */
    public DoubleAggregate(boolean ignoreSum, boolean ignoreAvg)
    {
      this.ignoreSum = ignoreSum;
      this.ignoreAvg = ignoreAvg;
    }

    /**
     * Add a double to the number set
     *
     * @param num the number
     */
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

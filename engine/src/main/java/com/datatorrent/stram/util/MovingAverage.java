/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

/**
 * Moving average calculations.
 */
public class MovingAverage
{
  public static class MovingAverageLong {
    private final int periods;
    private final long[] values;
    private int index = 0;
    private boolean filled = false;

    public MovingAverageLong(int periods) {
      this.periods = periods;
      this.values = new long[periods];
    }

    public void add(long val) {
      values[index++] = val;
      if (index == periods) {
        filled = true;
      }
      index %= periods;
    }

    public long getAvg() {
      long sum = 0;
      for (int i=0; i<periods; i++) {
        sum += values[i];
      }

      if (!filled) {
        return index == 0 ? 0 : sum/index;
      } else {
        return sum/periods;
      }
    }
  }

  // Generics don't work with numbers.  Hence this mess.
  public static class MovingAverageDouble {
    private final int periods;
    private final double[] values;
    private int index = 0;
    private boolean filled = false;

    public MovingAverageDouble(int periods) {
      this.periods = periods;
      this.values = new double[periods];
    }

    public void add(double val) {
      values[index++] = val;
      if (index == periods) {
        filled = true;
      }
      index %= periods;
    }

    public double getAvg() {
      double sum = 0;
      for (int i=0; i<periods; i++) {
        sum += values[i];
      }

      if (!filled) {
        return index == 0 ? 0 : sum/index;
      } else {
        return sum/periods;
      }
    }
  }

  public static class TimedMovingAverageLong {
    private final int periods;
    private final long[] values;
    private final long[] timeIntervals;
    private int index = 0;
    private final long baseTimeInterval;

    public TimedMovingAverageLong(int samples, long baseTimeInterval) {
      this.periods = samples;
      this.values = new long[samples];
      this.timeIntervals = new long[samples];
      this.baseTimeInterval = baseTimeInterval;
    }

    public void add(long val, long time) {
      values[index] = val;
      timeIntervals[index] = time;
      index++;
      index %= periods;
    }

    public double getAvg() {
      long sumValues = 0;
      long sumTimeIntervals = 0;
      int i = index;
      while (true) {
        i--;
        if (i < 0) {
          i = periods - 1;
        }
        if (i == index) {
          break;
        }
        sumValues += values[i];
        sumTimeIntervals += timeIntervals[i];
        if (sumTimeIntervals >= baseTimeInterval) {
          break;
        }
      }

      if (sumTimeIntervals == 0) {
        return 0;
      }
      else {
        return ((double)sumValues * 1000) / sumTimeIntervals;
      }
    }
  }

}

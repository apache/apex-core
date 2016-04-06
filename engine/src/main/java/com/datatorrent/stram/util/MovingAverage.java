/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.util;

/**
 * Moving average calculations.
 *
 * @since 0.9.1
 */
public class MovingAverage
{
  public static class MovingAverageLong implements java.io.Serializable
  {
    private static final long serialVersionUID = 201404291550L;
    private final int periods;
    private final long[] values;
    private int index = 0;
    private boolean filled = false;

    public MovingAverageLong(int periods)
    {
      this.periods = periods;
      this.values = new long[periods];
    }

    public synchronized void add(long val)
    {
      values[index++] = val;
      if (index == periods) {
        filled = true;
      }
      index %= periods;
    }

    public synchronized long getAvg()
    {
      long sum = 0;
      for (int i = 0; i < periods; i++) {
        sum += values[i];
      }

      if (!filled) {
        return index == 0 ? 0 : sum / index;
      } else {
        return sum / periods;
      }
    }
  }

  // Generics don't work with numbers.  Hence this mess.
  public static class MovingAverageDouble implements java.io.Serializable
  {
    private static final long serialVersionUID = 201404291550L;
    private final int periods;
    private final double[] values;
    private int index = 0;
    private boolean filled = false;

    public MovingAverageDouble(int periods)
    {
      this.periods = periods;
      this.values = new double[periods];
    }

    public synchronized void add(double val)
    {
      values[index++] = val;
      if (index == periods) {
        filled = true;
      }
      index %= periods;
    }

    public synchronized double getAvg()
    {
      double sum = 0;
      for (int i = 0; i < periods; i++) {
        sum += values[i];
      }

      if (!filled) {
        return index == 0 ? 0 : sum / index;
      } else {
        return sum / periods;
      }
    }
  }

  public static class TimedMovingAverageLong implements java.io.Serializable
  {
    private static final long serialVersionUID = 201404291550L;
    private final int periods;
    private final long[] values;
    private final long[] timeIntervals;
    private int index = 0;
    private final long baseTimeInterval;

    public TimedMovingAverageLong(int samples, long baseTimeInterval)
    {
      this.periods = samples;
      this.values = new long[samples];
      this.timeIntervals = new long[samples];
      this.baseTimeInterval = baseTimeInterval;
    }

    public synchronized void add(long val, long time)
    {
      values[index] = val;
      timeIntervals[index] = time;
      index++;
      index %= periods;
    }

    public synchronized double getAvg()
    {
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
      } else {
        return ((double)sumValues) / sumTimeIntervals;
      }
    }
  }

}

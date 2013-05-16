/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class Combinations
{
  public static <K> List<List<K>> getCombinations(List<K> list, int r)
  {
    List<List<K>> result = new ArrayList<List<K>>();
    List<int[]> combos = getNumberCombinations(list.size(), r);

    for (int[] a: combos) {
      List<K> keys = new ArrayList<K>(r);
      for (int j = 0; j < r; j++) {
        keys.add(j, list.get(a[j]));
      }
      result.add(keys);
    }

    return result;
  }

  public static List<int[]> getNumberCombinations(int n, int r)
  {
    List<int[]> list = new ArrayList<int[]>();
    int[] res = new int[r];
    for (int i = 0; i < res.length; i++) {
      res[i] = i;
    }
    boolean done = false;
    while (!done) {
      int[] a = new int[res.length];
      System.arraycopy(res, 0, a, 0, res.length);
      list.add(a);
      done = next(res, n, r);
    }
    return list;
  }

  private static boolean next(int[] num, int n, int r)
  {
    int target = r - 1;
    num[target]++;
    if (num[target] > ((n - (r - target)))) {
      // Carry the One
      while (num[target] > ((n - (r - target) - 1))) {
        target--;
        if (target < 0) {
          break;
        }
      }
      if (target < 0) {
        return true;
      }
      num[target]++;
      for (int i = target + 1; i < num.length; i++) {
        num[i] = num[i - 1] + 1;
      }
    }
    return false;
  }

  public static void main(String[] args)
  {
    String[] list = new String[] {"a", "b", "c", "d", "e"};
    for (int i = 1; i <= list.length; i++) {
      System.out.println("Combinations: " + getCombinations(Arrays.asList(list), i));
    }
  }

}

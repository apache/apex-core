/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.physical;

import org.junit.Assert;

import org.junit.Test;

import com.datatorrent.stram.plan.physical.StatsRevisions.VersionedLong;

/**
 *
 */
public class StatsRevisionsTest
{
  @Test
  public void test()
  {
    StatsRevisions revs = new StatsRevisions();
    VersionedLong vl = revs.new VersionedLong();

    long v = vl.get();
    Assert.assertTrue("initial value", v == 0);

    revs.checkout();
    vl.set(5);

    v = vl.get();
    Assert.assertTrue("new value after set", v == 5);

    revs.commit();

    v = vl.get();
    Assert.assertTrue("new value after commit", v == 5);

    try {
      vl.set(5);
      Assert.fail("modify readonly revision");
    } catch (AssertionError ve) {
      // expected
    }

  }

}

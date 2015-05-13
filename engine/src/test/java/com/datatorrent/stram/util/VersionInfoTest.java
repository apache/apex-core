/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class VersionInfoTest
{

  @Test
  public void testCompareVersion()
  {
    int c = VersionInfo.compare("1.0", "1.1");
    Assert.assertTrue(c < 0);
    c = VersionInfo.compare("1.10", "1.2");
    Assert.assertTrue(c > 0);
    c = VersionInfo.compare("1.0", "1.0");
    Assert.assertTrue(c == 0);
    c = VersionInfo.compare("1.0-SNAPSHOT", "1.0");
    Assert.assertTrue(c == 0);
    c = VersionInfo.compare("asdb", "1.0");
    Assert.assertTrue(c < 0);
  }

}

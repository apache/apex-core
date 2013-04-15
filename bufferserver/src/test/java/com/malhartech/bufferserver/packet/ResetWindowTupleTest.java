/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ResetWindowTupleTest
{
  public ResetWindowTupleTest()
  {
  }

  @Test
  public void testGetSerializedTuple()
  {
    byte[] serial = ResetWindowTuple.getSerializedTuple(0x7afebabe, 500);
    ResetWindowTuple tuple = (ResetWindowTuple)Tuple.getTuple(serial, 0, serial.length);

    Assert.assertEquals(tuple.getBaseSeconds(), 0x7afebabe, "base seconds");
    Assert.assertEquals(tuple.getWindowWidth(), 500, "window width");
  }
}
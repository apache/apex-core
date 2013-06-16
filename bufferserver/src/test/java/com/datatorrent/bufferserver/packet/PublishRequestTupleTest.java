/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.bufferserver.packet.Tuple;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PublishRequestTupleTest
{
  public PublishRequestTupleTest()
  {
  }

  @Test
  public void testGetSerializedRequest()
  {
    String pubId = "TestPublisher";
    long windowId = 0xcafebabe000000ffL;
    byte[] serial = PublishRequestTuple.getSerializedRequest(null, pubId, windowId);
    PublishRequestTuple request = (PublishRequestTuple)Tuple.getTuple(serial, 0, serial.length);

    Assert.assertEquals(request.identifier, pubId, "Identifier");
    Assert.assertEquals(Long.toHexString((long)request.baseSeconds << 32 | request.windowId), Long.toHexString(windowId), "Window");
  }

}
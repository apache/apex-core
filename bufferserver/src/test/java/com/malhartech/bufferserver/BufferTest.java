/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.InvalidProtocolBufferException;
import com.malhartech.bufferserver.Buffer.Message;
import junit.framework.TestCase;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class BufferTest extends TestCase
{
  public BufferTest(String testName)
  {
    super(testName);
  }

  @Override
  protected void setUp() throws Exception
  {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception
  {
    super.tearDown();
  }

  public void testSerDe() throws InvalidProtocolBufferException
  {
    Message.Builder db = Message.newBuilder();
    db.setType(Message.MessageType.NO_MESSAGE);
    db.setWindowId(0);

    Message d = db.build();

    byte[] serialized = d.toByteArray();

    db = Message.newBuilder().mergeFrom(serialized, 0, serialized.length);
    Message d1 = db.build();

    assertEquals(d, d1);
  }

}

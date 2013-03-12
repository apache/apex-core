/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.google.protobuf.InvalidProtocolBufferException;
import com.malhartech.bufferserver.Buffer.Message;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    logger.info("testSerDe");
    Message.Builder db = Message.newBuilder();
    db.setType(Message.MessageType.NO_MESSAGE);

    Message d = db.build();

    byte[] serialized = d.toByteArray();

    db = Message.newBuilder().mergeFrom(serialized, 0, serialized.length);
    Message d1 = db.build();

    assertEquals(d, d1);
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferTest.class);
}

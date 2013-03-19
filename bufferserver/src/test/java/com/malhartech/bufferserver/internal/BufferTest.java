/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.bufferserver.packet.NoMessageTuple;
import com.malhartech.bufferserver.packet.Tuple;
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

  public void testSerDe()
  {
    logger.info("testSerDe");

    byte[] serialized = NoMessageTuple.getSerializedTuple();
    Tuple t = Tuple.getTuple(serialized, 0, serialized.length);

    assert(t.getType() == MessageType.NO_MESSAGE);
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferTest.class);
}

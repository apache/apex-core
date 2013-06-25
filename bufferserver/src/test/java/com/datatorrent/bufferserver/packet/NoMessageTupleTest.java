/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.NoMessageTuple;
import com.datatorrent.bufferserver.packet.Tuple;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class NoMessageTupleTest extends TestCase
{
  public NoMessageTupleTest(String testName)
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

  @Test
  public void testSerDe()
  {
    logger.info("testSerDe");

    byte[] serialized = NoMessageTuple.getSerializedTuple();
    Tuple t = Tuple.getTuple(serialized, 0, serialized.length);

    assert(t.getType() == MessageType.NO_MESSAGE);
  }

  private static final Logger logger = LoggerFactory.getLogger(NoMessageTupleTest.class);
}

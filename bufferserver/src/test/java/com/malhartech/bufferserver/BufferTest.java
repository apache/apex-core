/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.InvalidProtocolBufferException;
import com.malhartech.bufferserver.Buffer.Data;
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
    Data.Builder db = Data.newBuilder();
    db.setType(Data.DataType.NO_DATA);
    db.setWindowId(0);
    
    Data d = db.build();
    
    byte[] serialized = d.toByteArray();

    db = Data.newBuilder().mergeFrom(serialized, 0, serialized.length);
    Data d1 = db.build();

    assertEquals(d, d1);
  }

}

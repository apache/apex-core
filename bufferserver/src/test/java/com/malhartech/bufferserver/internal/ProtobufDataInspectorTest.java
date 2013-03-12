/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.internal.ProtobufDataInspector;
import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import com.malhartech.bufferserver.util.SerializedData;
import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ProtobufDataInspectorTest
{
  public ProtobufDataInspectorTest()
  {

  }

  public void testGetType()
  {
    logger.info("testGetType");
    SerializedData data = null;
    ProtobufDataInspector instance = new ProtobufDataInspector();
    MessageType expResult = null;
    MessageType result = instance.getType(data);
    assertEquals(expResult, result);
    fail("The test case is a prototype.");
  }

  public void testGetWindowId()
  {
    System.out.println("getWindowId");
    SerializedData data = null;
    ProtobufDataInspector instance = new ProtobufDataInspector();
    int expResult = 0;
    int result = instance.getWindowId(data);
    assertEquals(expResult, result);
    fail("The test case is a prototype.");
  }

  public void testGetData()
  {
    System.out.println("getData");
    SerializedData data = null;
    ProtobufDataInspector instance = new ProtobufDataInspector();
    Message expResult = null;
    Message result = instance.getData(data);
    assertEquals(expResult, result);
    fail("The test case is a prototype.");
  }

  public void testWipeData_SerializedData()
  {
    System.out.println("wipeData");
    SerializedData data = null;
    ProtobufDataInspector instance = new ProtobufDataInspector();
    instance.wipeData(data);
    fail("The test case is a prototype.");
  }

  @Test
  public void testWipeData_3args() throws IOException
  {
    System.out.println("wipeData");
    byte[] start = null;
    int offset = 0;
    int length = 0;
    ProtobufDataInspector.wipeData(start, offset, length);
    fail("The test case is a prototype.");
  }

  public void testGetBaseSeconds()
  {
    System.out.println("getBaseSeconds");
    SerializedData data = null;
    ProtobufDataInspector instance = new ProtobufDataInspector();
    int expResult = 0;
    int result = instance.getBaseSeconds(data);
    assertEquals(expResult, result);
    fail("The test case is a prototype.");
  }

  private static final Logger logger = LoggerFactory.getLogger(ProtobufDataInspectorTest.class);
}

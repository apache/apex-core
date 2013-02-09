/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import com.malhartech.bufferserver.util.SerializedData;
import org.junit.Test;
import static org.junit.Assert.*;

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
    System.out.println("getType");
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
  public void testWipeData_3args()
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

}
